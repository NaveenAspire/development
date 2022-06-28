"""This module will used to create the table for the config
file and store section values into the table and use them in script"""

import argparse
import ast

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
    subparsers = parser.add_subparsers()
    create_table = subparsers.add_parser('create_table')
    create_table.add_argument('params',type=ast.literal_eval)
    put_item = subparsers.add_parser('put_item')
    put_item.add_argument("table",type=str)
    put_item.add_argument("params",type=ast.literal_eval)
    get_item = subparsers.add_parser('get_item',type=str)
    get_item.add_argument('table')
    get_item.add_argument('params',type=ast.literal_eval)
    args = parser.parse_args()
    
    print(type(args.params))

if __name__ == "__main__":
    main()