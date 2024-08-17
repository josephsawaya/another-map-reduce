use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;

use crate::shared::Task;

use crate::component::Component;
use crate::rpc::{RPCError, RPCHandlerFunc, RPCHandlerReturn, RPCServer};

use glob::glob;

fn get_task(state: Arc<Mutex<CoordinatorState>>) -> Result<RPCHandlerReturn, RPCError> {
    let clone_of_state = state.clone();
    let mut b = state.lock().unwrap();

    b.refresh();
    let t_to_assign: Option<Task>;
    if b.map.todo.len() == 0 && b.map.curr.len() == 0 {
        // we can start to reduce
        t_to_assign = b.reduce.todo.pop();
    } else {
        t_to_assign = b.map.todo.pop();
    };

    if b.map.curr.len() == 0
        && b.map.todo.len() == 0
        && b.reduce.todo.len() == 0
        && b.reduce.curr.len() == 0
    {
        return Ok("done".to_string());
    }

    match t_to_assign {
        None => Ok("".to_string()),
        Some(t) => {
            let string_to_return = t.to_string();
            let orig_task_id = t.get_id();
            let orig_task_type = match &t {
                Task::Map { .. } => {
                    println!("Assigning Map task {}", orig_task_id);
                    b.map.curr.push(t);
                    "map"
                }
                Task::Reduce { .. } => {
                    println!("Assigning Reduce task {}", orig_task_id);
                    b.reduce.curr.push(t);
                    "reduce"
                }
            };

            spawn(move || {
                // wait for ten seconds
                // refresh the state
                // check if the task is done
                // if it is not done move it back to todo
                let time_to_sleep = Duration::from_secs(10);
                sleep(time_to_sleep);

                let mut new_b = clone_of_state.lock().unwrap();
                new_b.refresh();
                match orig_task_type {
                    "map" => {
                        let mut remove_it: Option<usize> = None;
                        for (i, t) in new_b.map.curr.iter().enumerate() {
                            if t.get_id() == orig_task_id {
                                println!(
                                    "Map task {} was not completed, replacing in todo",
                                    t.get_id()
                                );
                                remove_it = Some(i);
                            }
                        }

                        match remove_it {
                            Some(x) => {
                                let task = new_b.map.curr.remove(x);
                                new_b.map.todo.push(task);
                            }
                            None => {}
                        }
                    }
                    "reduce" => {
                        let mut remove_it: Option<usize> = None;
                        for (i, t) in new_b.reduce.curr.iter().enumerate() {
                            if t.get_id() == orig_task_id {
                                println!(
                                    "Map task {} was not completed, replacing in todo",
                                    t.get_id()
                                );
                                remove_it = Some(i);
                            }
                        }

                        match remove_it {
                            Some(x) => {
                                let task = new_b.reduce.curr.remove(x);
                                new_b.reduce.todo.push(task);
                            }
                            None => {}
                        }
                    }
                    _ => {}
                }
            });

            Ok(string_to_return)
        }
    }
}

pub struct CoordinatorState {
    map: StageState,
    reduce: StageState,
}

#[derive(Debug)]
struct StageState {
    todo: Vec<Task>,
    curr: Vec<Task>,
    done: Vec<Task>,
}

impl StageState {
    pub fn new() -> Self {
        StageState {
            todo: vec![],
            curr: vec![],
            done: vec![],
        }
    }
}

impl CoordinatorState {
    pub fn new() -> Self {
        CoordinatorState {
            map: StageState::new(),
            reduce: StageState::new(),
        }
    }

    pub fn refresh(&mut self) {
        let mut i = 0;
        while i < self.map.curr.len() {
            match self.map.curr[i] {
                Task::Map {
                    id, num_buckets, ..
                } => {
                    let paths: Vec<_> =
                        glob(format!("./files/intermediate_map_{}_reduce_*", id).as_str())
                            .unwrap()
                            .map(|x| x.unwrap())
                            .collect();
                    if paths.len() == num_buckets {
                        let removed_task = self.map.curr.remove(i);
                        println!("Map Task {:?} done", removed_task);
                        self.map.done.push(removed_task);
                    } else {
                        i = i + 1;
                    }
                }
                Task::Reduce { .. } => panic!("Error refreshing state, expected map got reduce"),
            }
        }

        i = 0;
        while i < self.reduce.curr.len() {
            match self.reduce.curr[i] {
                Task::Reduce { id } => {
                    let paths: Vec<_> = glob(format!("./files/reduce_result_{}", id).as_str())
                        .unwrap()
                        .map(|x| x.unwrap())
                        .collect();
                    if paths.len() == 1 {
                        let removed_task = self.reduce.curr.remove(i);
                        println!("Reduce Task {:?} done", removed_task);
                        self.reduce.done.push(removed_task);
                    } else {
                        i = i + 1;
                    }
                }
                Task::Map { .. } => panic!("Error refreshing state, expected reduce got map"),
            }
        }
    }
}

impl Debug for CoordinatorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State\nMap: todo: {:?} curr: {:?} done: {:?}\nReduce: todo: {:?} curr: {:?} done: {:?}\n",
            self.map.todo, self.map.curr, self.map.done, self.reduce.todo, self.reduce.curr ,self.reduce.done
        )
    }
}

pub struct Coordinator {
    pub num_buckets: u8,
    pub state: CoordinatorState,
    server: RPCServer<CoordinatorState>,
}

impl Component for Coordinator {
    fn start(self) {
        self.server.server(self.state).unwrap();
    }
}

impl Coordinator {
    pub fn new(num_buckets: u8) -> Coordinator {
        let mut functions: HashMap<String, RPCHandlerFunc<CoordinatorState>> = HashMap::new();
        functions.insert("get_task".to_string(), get_task);

        let d = glob("./files/*.txt").unwrap();
        let mut map_todo: Vec<Task> = vec![];
        for (idx, file) in d.into_iter().enumerate() {
            map_todo.push(Task::Map {
                id: idx,
                file: file.unwrap().to_string_lossy().into_owned(),
                num_buckets: usize::from(num_buckets),
            });
        }
        let mut reduce_todo: Vec<Task> = vec![];
        for idx in 0..num_buckets {
            reduce_todo.push(Task::Reduce {
                id: usize::from(idx),
            })
        }

        let mut state = CoordinatorState::new();

        state.map.todo = map_todo;
        state.reduce.todo = reduce_todo;

        let server = RPCServer { functions };
        Coordinator {
            num_buckets,
            server,
            state,
        }
    }
}
