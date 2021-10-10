use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;
use std::process;
use std::error::Error;
use std::collections::HashMap;
use std::env;
use std::sync::mpsc;
use std::thread;
use threadpool::ThreadPool;

struct Config {
    file_name: String,
}

impl Config {
    fn new(args: &[String]) -> Result<Config, &str> {
        if args.len() < 2 {
            return Err("Not enough arguments");
        }
        let file_name = args[1].clone();
        
        Ok(Config {file_name})
    }
}

fn parse(buffer_string: &str) -> HashMap<String, u64> {
    buffer_string
        .split_whitespace()
        .fold(HashMap::new(), |mut acc, word| {
            let word = word.trim_matches(|c: char| c.is_numeric() || c.is_ascii_punctuation()).to_lowercase();
            *acc.entry(word.to_string()).or_insert(0)+=1;
            acc
        })
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(&args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {}", err);
        process::exit(1);
    });

    let mut reader = BufReader::with_capacity(1024*64,File::open(config.file_name.to_string())?);
    let mut buffer_string = String::new();
    let (tx, rx) = mpsc::channel();
    let counter_thread = thread::spawn(move|| {
        let mut ans: HashMap<String, u64> = HashMap::new();
        for message in rx {
            for (key, cnt) in message {
                *ans.entry(key).or_insert(0)+=cnt;
            }
        }
        let mut writer = BufWriter::new(File::create(config.file_name + "_count.txt").unwrap());
        let _ = ans
            .iter()
            .filter(|(word, _)| !word.trim().is_empty())
            .map(|(word, count)| 
                writer
                    .write_all((word.to_string() + " " +  &count.to_string() + "\r\n").as_bytes()))
            .flatten()
            .collect::<Vec<_>>();
    });
    let workers = 4;
    let pool = ThreadPool::new(workers);
    while let Ok(size) = reader.read_line(&mut buffer_string) {
        if size == 0 {
            break;
        }
        let string = buffer_string.to_owned();
        let tx1 = tx.clone();
        pool.execute(move|| {
            tx1.send(parse(&string)).unwrap();
            drop(tx1);
        });
        buffer_string.clear();
        while pool.active_count() >= workers {}
    }
    drop(tx);
    counter_thread.join().unwrap();
    Ok(())
}
