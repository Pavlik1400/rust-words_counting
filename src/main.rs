use argparse::{ArgumentParser, Store};
use words_counting::wc_config::WCConfig;
use words_counting::count_words;

fn main() {
    let mut config_name = String::new();
    {
        let mut parser = ArgumentParser::new();
        parser.set_description("Recursively count words in all txt file in given direcory");
        parser.refer(&mut config_name)
            .add_option(&["-c", "--config"], Store, "Path to json config");
        parser.parse_args_or_exit();
    }
    if config_name.is_empty() {
        println!("Warning: using default config: config.json");
        config_name = String::from("config.json");
    }

    let config = WCConfig::new(config_name);
    let _counted_words = count_words(&config);
}
