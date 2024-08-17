use core::fmt;
use std::{
    collections::HashMap,
    fmt::Debug,
    io::Write,
    net::{TcpListener, TcpStream},
    process::exit,
    sync::{Arc, Mutex},
    thread::spawn,
};

#[derive(Debug, Clone)]
pub struct RPCError {}

impl fmt::Display for RPCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error with RPC")
    }
}

pub type RPCHandlerReturn = String;
pub type RPCHandlerFunc<T> = fn(Arc<Mutex<T>>) -> Result<RPCHandlerReturn, RPCError>;

pub struct RPCServer<T>
where
    T: Send + 'static,
{
    pub functions: HashMap<String, RPCHandlerFunc<T>>,
}

impl<T> RPCServer<T>
where
    T: Send + 'static + Debug,
{
    fn call_func(
        &self,
        state: Arc<Mutex<T>>,
        function_designation: String,
    ) -> Result<String, RPCError> {
        match self.functions.get(&function_designation) {
            Some(func) => {
                // start new thread that calls this function here
                match func(state) {
                    Ok(reply) => Ok(reply),
                    Err(_) => Err(RPCError {}),
                }
            }
            None => return Err(RPCError {}),
        }
    }

    fn handle_client(&self, state: Arc<Mutex<T>>, mut stream: TcpStream) {
        let function_designation = "get_task".to_string();

        let reply = match self.call_func(state, function_designation.clone()) {
            Err(_) => {
                println!("Error when calling function, {}", function_designation);
                exit(1);
            }
            Ok(x) => x,
        };

        // each connection should get its own thread so this should be fine
        match stream.write(reply.as_bytes()) {
            Err(_) => println!("Error writing to stream"),
            Ok(_) => (),
        };
    }

    pub fn server(self, state: T) -> std::io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:3000")?;

        let a = Arc::new(self);
        let s = Arc::new(Mutex::new(state));

        // accept connections and process them serially
        for stream in listener.incoming() {
            // should start a new thread here
            let str = stream?;
            let thing = a.clone();
            let other_thing = s.clone();

            spawn(move || thing.handle_client(other_thing, str));
        }
        Ok(())
    }
}

pub struct RPCClient {}
