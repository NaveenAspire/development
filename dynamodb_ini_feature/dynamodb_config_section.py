"""This module will used to create the table for the config
file and store section values into the table and use them in script"""

import argparse

class DynamodbConfig:
    """This is the class used to create the table for
    config file and store section values as item into the table"""
    
    def __init__(self) -> None:
        """This is the init method for the class DynamodbConfig"""
        pass
    

def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser used for get user input"
    )
    

if __name__ == "__main__":
    main()