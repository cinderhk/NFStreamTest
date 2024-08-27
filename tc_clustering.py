from nfstream import NFStreamer
import pandas as pd
import os
import glob


def remove_directory(dir_path):
    # 遍历指定目录中的所有文件和子目录
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        # 如果是文件，则直接删除
        if os.path.isfile(file_path):
            os.remove(file_path)
        # 如果是目录，则递归删除
        elif os.path.isdir(file_path):
            remove_directory(file_path)
    # 删除最外层的目录
    os.rmdir(dir_path)

def load():
    packet_path = "D:\\PycharmProjects\\NFStreamTest\\NFStreamTest\\pcaps\\"

    # 遍历指定目录下的所有文件
    i = 0
    os.makedirs('temp', exist_ok=True)

    for filename in os.listdir(packet_path):
        # 对每个文件进行处理，例如读取、分析等
        file_path = os.path.join(packet_path, filename)
        print(file_path)  # 打印文件路径，方便调试

        streamer = NFStreamer(source=file_path).to_pandas()[
            ['src_ip', 'src_port', 'dst_ip', 'dst_port', 'protocol',
             'requested_server_name', 'application_name']]
        i += 1
        print(streamer)

        streamer.to_csv('temp' + '\\' + str(i) + '.csv', index=False)

    # 找到所有要合并的CSV文件
    csv_files = glob.glob("temp/*.csv")

    # 将CSV文件读入Pandas DataFrame对象
    df_list = []
    for filename in csv_files:
        df = pd.read_csv(filename)
        df_list.append(df)

    # 合并所有DataFrame对象
    merged_df = pd.concat(df_list, axis=0, ignore_index=True)

    # 将合并后的DataFrame保存为新的CSV文件
    merged_df.to_csv("flow_data.csv", index=False, encoding='utf-8')

    print(streamer)
    remove_directory('temp')
    dfrow = pd.read_csv('flow_data.csv')

    dfrow.to_csv("flow_data.csv", index=False, encoding='utf-8')


def remove_duplicates_from_column(input_file, output_file, column_name):
    # 读取CSV文件
    df = pd.read_csv(input_file)

    # 去除指定列的重复项
    df_unique = df.drop_duplicates(subset=[column_name], keep='first')

    # 保存去重后的数据到新的CSV文件
    df_unique.to_csv(output_file, index=False)

def process_flow_data(filename):
    streamer = NFStreamer(source=filename).to_pandas()[
        ['src_ip', 'src_port', 'dst_ip', 'dst_port', 'protocol',
         'requested_server_name','application_name']]
    streamer.head(5)

    streamer.to_csv('flow_data_new.csv')

    print(streamer)



if __name__ == '__main__':
    #load()
    #process_flow_data('pcaps/test1.pcap')
    remove_duplicates_from_column('flow_data.csv','flow_data_unique.csv','requested_server_name')






