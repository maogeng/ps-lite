#include "ps/ps.h"

using namespace ps;

template<class Val>

class MyKVServerHandle {
public:
		void operator() (const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
				size_t n = req_data.keys.size();
				KVPairs<Val> res;
				if (req_meta.push) {
						CHECK_EQ(n, req_data.lens.size());
				} else {
						res.keys = req_data.keys;
						res.lens.resize(res.keys.size());
				}

				size_t cur_idx = 0;
				for(size_t i = 0; i < n; i++) {
						Key key = req_data.keys[i];
						if(req_meta.push) {
								int len = req_data.lens[i];
								if(store.count(key) == 0) {
										store[key] = vector<Val>(len,0);
								}

								for(int idx = 0; idx < len; ++idx) {
										store[key][idx] += req_data.vals[cur_idx++];
								}
						} else {
								res.lens[i] = store[key].size();
								for(int idx = 0; idx < res.lens[i]; ++idx) {
										res.vals.push_back(store[key][idx]);
								}
						}
				}
				server->Response(req_meta, res);
		}
private:
		std::unordered_map<Key, vector<Val>> store;
}

void StartServer() {
		if(!IsServer()) return;
		cout << "num of workers[" << NumWorkers() << "]" << endl;
		cout << "num of servers[" << NumServers() << "]" << endl;
		auto server = new KVServer<float>(0);
		server->set_request_handle(MyKVServerHandle<float>());
		RegisterExitCallback([server]() { delete server;});
}

void RunWorker() {
		if (!IsWorker()) return;
		cout << "start Worker rank = " << MyRank() << endl;
		KVWorker<float> kv(0);

		int key_num = 10;
		int val_num = 0;
		vector<Key> keys(key_num);
		vector<int> len(key_num);
		for(int i = 0; i < key_num; ++i) {
				keys[i] = i;
				len[i] = i + 1;
				val_num += len[i];
		}

		vector<float> vals(val_num);
		for(int i = 0; i< val_num; ++i) {
				vals[i] = i / 10;
		}

		int repeat = 10;
		vector<int> ts;
		for (int i = 0; i < repeat; ++i) {
				ts.push_back(kv.Push(keys, vals, len));
		}

		for (int t: ts) kv.Wait(t);
}

int main(int argc, char *argv[]) {
		std::vector<uint64_t> key = {1, 3, 5};
		std::vector<float> val = {1, 1, 1};
		std::vector<float> recv_val;
		KVWorker<float> w(0);
		w.Wait(w.Push(key, val));
		w.Wait(w.Pull(key, &recv_val));
		for(int i = 0; i < recv_val.size(); i ++) {
				std::cout<<recv_val[i] << std::endl;
		}
		return 0;
}
