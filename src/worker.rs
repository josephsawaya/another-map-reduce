use glob::glob;
use regex::Regex;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
    net::TcpStream,
    process::exit,
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use crate::{component::Component, shared::Task};

pub struct Worker {
    pub worker_id: u8,
}

fn map(v: String) -> Vec<(String, String)> {
    let mut b: Vec<(String, String)> = Vec::new();

    let re = Regex::new(r"[^A-Za-z]").unwrap();
    let result = re.replace_all(&v, " ");

    let words = result.split_whitespace();
    for word in words {
        b.push((word.to_string(), 1.to_string()));
    }
    b
}

fn reduce(k: String, vv: Vec<String>) -> (String, String) {
    (k, vv.len().to_string())
}

fn random_string(n: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from) // From link above, this is needed in later versions
        .collect()
}

fn get_time() -> Duration {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

impl Component for Worker {
    fn start(self) {
        let mut task: Task;
        loop {
            println!(
                "Worker id {} asked for task {:?}",
                self.worker_id,
                get_time()
            );

            let mut t = TcpStream::connect("127.0.0.1:3000").unwrap();
            let mut reply = String::new();
            t.read_to_string(&mut reply).unwrap();

            if reply == "" {
                println!("No tasks yet, waiting 10 microseconds and checking again");
                let duration_of_sleep = Duration::from_micros(10);
                sleep(duration_of_sleep);
                continue;
            } else if reply == "done" {
                exit(1);
            };

            task = Task::from_string(reply);

            println!(
                "Worker id {} Starting Task {:?} Time {:?}",
                self.worker_id,
                task,
                get_time()
            );

            match task {
                Task::Map {
                    id,
                    ref file,
                    num_buckets,
                } => {
                    let contents = fs::read_to_string(file).unwrap();
                    let mut map_result = map(contents);
                    let (curr_key, mut curr_bucket) = ("", 0);
                    let mut buckets: Vec<Vec<(String, String)>> = vec![vec![]; num_buckets];
                    map_result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                    for (k, v) in map_result {
                        if k != curr_key {
                            let mut hasher = DefaultHasher::new();
                            k.hash(&mut hasher);
                            curr_bucket = usize::try_from(hasher.finish()).unwrap() % num_buckets;
                        }
                        buckets[curr_bucket].push((k, v));
                    }
                    for i in 0..num_buckets {
                        let temp_file_name = format!("temp_file_{}", random_string(8));
                        let file_to_write_to =
                            format!("./files/intermediate_map_{}_reduce_{}", id, i);
                        let mut file = File::create(&temp_file_name).unwrap();
                        for (k, v) in &buckets[i] {
                            file.write(format!("{} {}\n", k, v).as_bytes()).unwrap();
                        }
                        fs::rename(temp_file_name, file_to_write_to).unwrap();
                    }
                }
                Task::Reduce { id } => {
                    let mut b: BTreeMap<String, Vec<String>> = BTreeMap::new();

                    for entry in
                        glob(format!("./files/intermediate_map_*_reduce_{}", id).as_str()).unwrap()
                    {
                        let e = entry.unwrap();
                        let contents = fs::read_to_string(e).unwrap();
                        let lines = contents.split('\n');

                        for line in lines {
                            let c: Vec<String> = line.split(' ').map(|x| x.to_string()).collect();
                            if c.len() != 2 {
                                // skip the line i guess
                                continue;
                            }
                            let (k, v) = (c[0].clone(), c[1].clone());

                            match b.get_mut(&k) {
                                None => {
                                    b.insert(k, vec![v]);
                                }
                                Some(x) => x.push(v),
                            };
                        }
                    }

                    let temp_file_name = format!("temp_file_{}", random_string(8));
                    let file_to_write_to = format!("./files/reduce_result_{}", id);
                    let mut file = File::create(&temp_file_name).unwrap();

                    for (k, v) in b {
                        let (reduce_key, reduce_val) = reduce(k.clone(), v);
                        file.write(format!("{} {}\n", reduce_key, reduce_val).as_bytes())
                            .unwrap();
                    }

                    fs::rename(temp_file_name, file_to_write_to).unwrap();

                    for entry in
                        glob(format!("./files/intermediate_map_*_reduce_{}", id).as_str()).unwrap()
                    {
                        let e = entry.unwrap();
                        fs::remove_file(e).unwrap();
                    }
                }
            }
            println!(
                "Finished Worker id {} Task {:?} Time {:?}",
                self.worker_id,
                task,
                get_time()
            );
        }
    }
}
