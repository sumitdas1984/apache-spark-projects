'''
creates a pandas dataframe from the blogs xml files. The output file is in the form of a json.

python create_dataset.py <input> <output>


Load the json file created by using:
    import pandas as pd
    pd.read_json(<output>,orient='records')

'''

import sys
import os
import pandas as pd
import logging
import traceback

logger = logging.getLogger(__name__)

def extract_all_posts(file_path):
    with open(file_path,'rb') as blog:
        text_str = blog.read()
        text_str = str(text_str)
        start = text_str.find('<post>')
        end = text_str.find('</post>')
        parsed = []
        while end!=-1:
            start = start+6
            end = end
            post = text_str[start:end]
            parsed.append(post)
            start = text_str.find('<post>',end+1)
            end = text_str.find('</post>',start)
        parsed_text = '\n'.join(parsed)
        parsed_text = parsed_text.strip()
        return parsed_text



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
            text = extract_all_posts(file_path)
            author_id,gender,age,industry,astrological_sign,_= [f.strip() for f in file_name.split(".")]
            row = dict(file_name=file_name,author_id=author_id, gender=gender,age=age,industry=industry, astrological_sign=astrological_sign, text=text)
            dataset.append(row)
            print("file {} processed".format(file_name))
            success += 1
        except Exception:
            traceback.print_exc(file=sys.stdout)
            failed += 1
        total += 1
        print("Total {} Success {} Failed {}".format(total, success, failed))
    dataframe = pd.DataFrame(dataset)
    return dataframe




if __name__ == '__main__':
    # df = create_dataset('data/blogs')
    # df.to_json('corpus.json',orient='records')
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    df = create_dataset(input_path)
    df.to_json(output_path,orient='records')
    



