import json
from os import path
import sys
import xml.etree.ElementTree as ET

try:
    
    file_path = sys.argv[1]
    
    if path.exists(file_path):
        print(f'FOUND FILE : {file_path}')
    else:
        print("NOT A VALID FILE PATH")
        sys.exit(1)

except Exception as err:
    print(f'ERROR: {err}')
    sys.exit(1)


file_ = open(file_path, 'rb')

context = ET.iterparse(file_, events=('start', 'end'))
context = iter(context)

file_number = 0
na_number = 0
albtrck = {}
check_points = [x for x in range(0, 1000000000001, 100000)]
check = 0
iters = 0

for event, ele in context:
    
    # indicates end of album info
    if event == 'end' and ele.tag == 'master':
        
        if iters > check_points[check]: #print checkpoint of iterations and current objet size
            print(check_points[check])
            cur_size = sys.getsizeof(str(albtrck))
            check += 1
        else:
            cur_size = 0

        iters += 1
        
        try:
            # Make new file when over .24 GBs
            if cur_size > 240000000:
                print('S P L I T')
                fname = path.splitext(path.basename(path.normpath(file_path)))[0] + f'_{file_number}.json'
            
                with open(fname, 'w') as outfile:
                    print(f'CREATING FILE: {fname}')
                    json.dump(albtrck, outfile)
                    outfile.close()
                
                file_number += 1
                albtrck = {}
            
            else:
                continue
        
        except Exception as err:
            print(err)
            continue

        finally:
            ele.clear() # reduce memory usage by clearing extracted elements
    
    #indicates begining of album info 
    elif event == 'start' and ele.tag == 'master':
        alb = ''
        continue

    #indicates artist name
    elif event == 'start' and ele.tag == 'name':
        artist = ele.text
        if artist in albtrck.keys():
            continue
        else:
            albtrck[artist] = {}
    
    #indicates album title
    elif event == 'start' and ele.tag == 'title' and alb == '':
        alb = ele.text
        if not alb:
            alb = f"not_available_{na_number}"
            na_number += 1
        albtrck[artist][alb] = []
        continue
    
    #indicates track title
    elif event == 'start' and ele.tag == 'title' and len(alb) > 0:
        albtrck[artist][alb].append(ele.text)


fname = path.splitext(path.basename(path.normpath(file_path)))[0] + f'_{file_number}.json'

# create final file from remaining data
with open(fname, 'w') as outfile:
    print(f'CREATING FILE: {fname}')
    json.dump(albtrck, outfile)
    outfile.close()

file_.close()