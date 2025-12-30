import os

from src.processor import LogProcessor
from src.rules import ErrorsPerTimeRule, ErrorsPerTimeAndAttributeRule
from src.config import config

def main():
    """Main controller of the module for activating data processing"""
    data_path = os.getenv('DATA_PATH', 'data/data.csv')
    
    active_rules = [
        ErrorsPerTimeRule(config),
        ErrorsPerTimeAndAttributeRule(config)
    ]
    
    processor = LogProcessor(
        file_path=data_path,
        rules=active_rules,
        config=config
    )
    
    processor.process()

if __name__ == "__main__":
    main()
