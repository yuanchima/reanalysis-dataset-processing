import time
import os
import concurrent.futures

def download(url):
    cmd = r'wget -N -nv --load-cookies C:\Users\yuanchima\Downloads\Documents\.urs_cookies --save-cookies C:\Users\yuanchima\Downloads\Documents\.urs_cookies --auth-no-challenge=on --keep-session-cookies --user=yuanchima --password=earthdata_M16384 --content-disposition -i'
    cmd = cmd + ' ' + url
    os.system(cmd)

def load_url(file_path):
    with open(file_path, 'r') as f:
        all_url = f.readlines()
    all_url = [url.replace("\n", "") for url in all_url]
    return all_url

def split_file(all_url, file_nums, subset_root='./subset'):
    if os.path.exists(subset_root):
        files = os.listdir(subset_root)
        for f in files:
            os.remove(f"{subset_root}/{f}")
            # print(f'remove {f}')
    else:
        os.mkdir(subset_root)

    rows_per_file = len(all_url) // file_nums

    for i in range(file_nums):
        with open(f'{subset_root}/subset_{i}.txt', 'w') as f:
            if i != rows_per_file-1:
                f.writelines([f'{url}\n' for url in all_url[i*rows_per_file: (i+1)*rows_per_file]])
                # print(f'create {subset_root}/subset_{i}.txt')
            else:
                f.writelines([f'{url}\n' for url in all_url[i*rows_per_file:]])
                # print(f'create {subset_root}/subset_{i}.txt')
        

def threadPoolExecutor(file_list, num_worker=5):
    with concurrent.futures.ThreadPoolExecutor(num_worker) as executor:
        results = [executor.submit(download, f) for f in file_list]
        for f in concurrent.futures.as_completed(results):
            print(f.result())


if __name__ == "__main__":
    start = time.perf_counter()
    file_path = 'subset_M2T1NXSLV_5.12.4_20210103_113249.txt'
    all_url = load_url(file_path)
    subset_root = './subset'
    split_file(all_url, 100, subset_root)
    f_lst= [f'{subset_root}/{f}' for f in os.listdir(subset_root)]
    # print(f_lst)
    # threadPoolExecutor(f_lst, num_worker=5)
    download(f_lst[0])

    finish = time.perf_counter()
    print(f'Finished in {round(finish-start, 2)} second(s)')