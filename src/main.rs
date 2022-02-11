use argparse::{ArgumentParser, Store};
use std::time;
use words_counting::wc_config::WCConfig;
use words_counting::{count_words, dump_res};

fn main() {
    let mut config_name = String::new();
    {
        let mut parser = ArgumentParser::new();
        parser.set_description("Recursively count words in all txt file in given direcory");
        parser.refer(&mut config_name).add_option(
            &["-c", "--config"],
            Store,
            "Path to json config",
        );
        parser.parse_args_or_exit();
    }
    if config_name.is_empty() {
        println!("Warning: using default config: config.json");
        config_name = String::from("config.json");
    }

    let config = WCConfig::new(config_name);
    let count_result = count_words(&config);

    let start_time = time::Instant::now();
    dump_res(&config, &count_result);

    println!(
        "Total: {}ms\nReading: {}ms\nIndexing: {}ms\nSaving: {}ms",
        count_result.total_time,
        count_result.reading_time_ms,
        count_result.indexing_time_ms,
        start_time.elapsed().as_millis()
    );
}
