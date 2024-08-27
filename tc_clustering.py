from nfstream import NFStreamer

def pcapconversion(filename):
    streamer = NFStreamer(source=filename).to_pandas()[
        ['src_ip', 'src_port', 'dst_ip', 'dst_port', 'protocol',
         'requested_server_name','application_name']]
    streamer.head(5)

    streamer.to_csv('flow_data_new.csv')

    print(streamer)



if __name__ == '__main__':
    #load()
    pcapconversion('pcaps/test1.pcap')






