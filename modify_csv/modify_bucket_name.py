"""This module had script for modify bucket name in s3 path of csv file"""
import csv
import argparse
import os


class ModifyFile:
    """This is the ModifyFile class in the module"""
    def __init__(self, bucket_name) :
        """This is the init method of class ModifyFile"""
        self.bucket_name = bucket_name
        path = os.path.join(os.getcwd(),'opt/data')
        print(path)
        if not os.path.exists(path):
            os.makedirs(path)
        
        
    def check_bucket_name(self):
        """This method that check if the bucket name need to change in s3 path of csv file"""
        if self.bucket_name :
            self.change_csv_file()
        
    def change_csv_file(self) :
        """This method modify the csv file"""
        with open('aws_details.csv', 'r') as file :
            csv_reader = csv.DictReader(file)
            with open('opt/data/aws_details_updated.csv', 'w') as new_file:
                csv_writer = csv.DictWriter(new_file,fieldnames=csv_reader.fieldnames)
                csv_writer.writeheader()
                for row in csv_reader:
                    row['s3_path'] = self.change_bucket_name(row['s3_path'])
                    csv_writer.writerow(row)
                    
    
    def change_bucket_name(self,s3_path):
        """This method that change the bucket name in s3 path of csv file"""
        s3_bucket = 's3://'+self.bucket_name
        folder_path = s3_path.split('/',3)[-1]
        s3_path = "{0}/{1}".format(s3_bucket,folder_path)
        return s3_path
        

def main() :
    parser = argparse.ArgumentParser(description='This argparser used for getiing input')
    parser.add_argument('--bucket_name', type=str, help='Enter the bccket name you need to change')
    args = parser.parse_args()
    mf = ModifyFile(args.bucket_name)
    # mf.change_bucket_name('s3://msg-datalake-us-east-1-dev/appetize-api/source/')
    mf.check_bucket_name()
    
if __name__ == '__main__' :
    main()    
        