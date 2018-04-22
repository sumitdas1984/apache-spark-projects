'''
creates a pandas dataframe from the blogs xml files. The output file is in the form of a json.

python create_dataset.py <path-to-dir-containing-xml> <output-json-file-path>

example: python create_dataset.py data/blogs/ output.json

'''

import sys
import os
import json
import logging
import traceback
import random

logger = logging.getLogger(__name__)

def extract_all_posts(file_path):
    with open(file_path,'rb') as blog:
        text_str = blog.read()
        text_str = str(text_str)
        start = text_str.find('<post>')
        end = text_str.find('</post>')
        posts = []
        while end!=-1:
            start = start+6
            end = end
            post = text_str[start:end]
            posts.append(post)
            start = text_str.find('<post>',end+1)
            end = text_str.find('</post>',start)
        return posts

def create_rows(posts,info):
    
    rows = []
    for post in posts:
        item = info.copy()
        item.update({'text':post})
        rows.append(item)
    return rows



def get_all_file_paths(dir_path):
    file_names = os.listdir(dir_path)
    file_paths = {file_name: os.path.join(dir_path,file_name) for file_name in file_names}
    return file_paths

def create_dataset(dir_path):

    file_paths = get_all_file_paths(dir_path)
    dataset = []
    failed,success,total = 0,0,0
    for file_name,file_path in file_paths.items():
        try:
            print("processing file {} at location {}".format(file_name,file_path))
            all_posts = extract_all_posts(file_path)
            author_id,gender,age,industry,astrological_sign,_= [f.strip() for f in file_name.split(".")]
            info = dict(file_name=file_name,author_id=author_id, gender=gender,age=age,industry=industry, astrological_sign=astrological_sign)
            rows = create_rows(all_posts,info)
            dataset.extend(rows)
            print("file {} processed".format(file_name))
            success += 1
        except Exception:
            traceback.print_exc(file=sys.stdout)
            failed += 1
        total += 1
        print("Total {} Success {} Failed {}".format(total, success, failed))
    return dataset



if __name__ == '__main__':

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    dataset = create_dataset(input_path)
    
    # random.shuffle(dataset)
    # dataset = dataset[:10000]
    with open(output_path,'w') as output_file:
        json.dump(dataset,output_file)    




