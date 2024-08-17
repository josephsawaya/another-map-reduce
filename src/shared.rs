use std::fmt::Debug;

pub enum Task {
    Map {
        id: usize,
        file: String,
        num_buckets: usize,
    },
    Reduce {
        id: usize, // it can deduce the file names from just the id, needs
    },
}

impl Task {
    pub fn from_string(s: String) -> Self {
        let args: Vec<&str> = s.split('\n').collect();

        match args[0] {
            "map" => {
                assert!(args.len() == 4);
                Task::Map {
                    id: args[1].parse::<usize>().unwrap(),
                    file: args[2].to_string(),
                    num_buckets: args[3].parse::<usize>().unwrap(),
                }
            }
            "reduce" => {
                assert!(args.len() == 2);
                Task::Reduce {
                    id: args[1].parse::<usize>().unwrap(),
                }
            }
            _ => panic!("worker reply does not start with task type "),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Task::Map {
                id,
                file,
                num_buckets,
            } => {
                format!("map\n{}\n{}\n{}", id, file, num_buckets)
            }
            Task::Reduce { id } => {
                format!("reduce\n{}", id)
            }
        }
    }

    pub fn get_id(&self) -> usize {
        match self {
            Task::Map { id, .. } => *id,
            Task::Reduce { id } => *id,
        }
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::Map {
                id,
                file,
                num_buckets,
            } => {
                write!(
                    f,
                    "Map id: {} payload: {} num_buckets: {}",
                    id, file, num_buckets,
                )
            }
            Task::Reduce { id } => {
                write!(f, "Reduce id: {} ", id,)
            }
        }
    }
}
