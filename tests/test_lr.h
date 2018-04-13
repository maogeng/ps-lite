#ifndef __TEST_LR__
#define __TEST_LR__
#include <vector>

using namespace ps;

#define MAX_STRING 100
#define EXP_TABLE_SIZE 1000
#define MAX_EXP 6
#define COORDINATE_INFORM 16
#define MSG_INFORM 1
#define BUFFER_SIZE 100000
const char* seq = " \t";
const char* LABEL = "*LBL*";

extern float expTable[EXP_TABLE_SIZE];

struct ExpTable {
public:
	float table[EXP_TABLE_SIZE];
	static ExpTable& getInstance() {
		static ExpTable expTable;
		return expTable;
	}
private:
	ExpTable() {
		for(int i = 0; i < EXP_TABLE_SIZE; ++i) {
			table[i] = (float) exp((i / (float) EXP_TABLE_SIZE * 2 - 1) * MAX_EXP);
			table[i] = table[i] / (table[i] + 1);
		}
	}
};

struct RawSample {
	char buf[BUFFER_SIZE];
};

struct Data {
public:
	Data(int minibatchSize) : m_sampleCount(0), samples(minibatchSize) {
	}

	int m_sampleCount;
	std::vector<RawSample> samples;
};

Data * CreateData(int minibatchSize) {
	return new Data(minibatchSize);
}

struct id_value {
	Key id;
	float value;
	int sample;
	id_value(Key id_ = 0, float value_ = 0, int sample_ = -1): id(id_), value(value_), sample(sample_){}
};

float ParseWordWeight(char* word, int size, char* buffer) {
	char* tok = NULL, *str = NULL, *saveptr = NULL;
	int i = 0;
	float weight = 1.0;

	if (buffer[0] == ':') {
		word[0] = '\0';
		return -1;
	}

	for (str = buffer; (tok = strtok_r(str, ":", &saveptr)) != NULL && i < 2; str = NULL, ++i ) {
		if (i == 0) {
			strncpy(word, tok, size - 1);
			word[size - 1] = '\0';
		} else if (i == 1) {
			weight = atof(tok);
		}
		return weight;
	}
}

Key GetWordHash(const char* word) {
	Key k;
	std::istringstream iss;
	iss.str(word);
	iss >> k;
	return k;
}

void ParseData(Data* pData, std::vector<id_value>& all_keys, std::vector<Key>& unique_keys, std::vector<float>& label) {
	int mini_batch_sample_count = pData->m_sampleCount;
	float weight;
	char* thread_buf = NULL;
	char *str = NULL, *saveptr = NULL, *tok = NULL;
	char word[MAX_STRING];
	Key word_id;

	label.resize(mini_batch_sample_count, -1);

	for (int i = 0; i < mini_batch_sample_count; ++i) {
		thread_buf = pData->samples[i].buf;
		thread_buf[strcspn(thread_buf, "\n")] = '\0';
		for (str = thread_buf; (tok = strtok_r(str,seq, &saveptr)) != NULL; str =NULL) {
			weight = ParseWordWeight(word, MAX_STRING, tok);
			if (strcmp(word, LABEL) == 0) {
				label[i] = weight;
				continue;
			}
			word_id = GetWordHash(word);
			if (weight + 0 != weight) {
				continue;
			}
			all_keys.push_back(id_value(word_id, weight, i));
			unique_keys.push_back(word_id);
		}
	}
}


void DeleteData(Data* data) {
	delete data;
}

int GetSampleCount(const Data* data) {
	return data->m_sampleCount;
}

class DataQ
{
public:
	DataQ(int bufferNumber, int minibatchSize) : m_bufferNumber(bufferNumber), m_minibachSize(minibatchSize){
		for(int i = 0; i < m_bufferNumber; ++i) {
			m_emptyQ.push(CreateData(m_minibachSize));
		}
	}

	Data* PopEmptyData() {
		std::unique_lock<std::mutex> lck(m_mtxEmpty);
		m_condEmpty.wait(lck, [this] { return m_emptyQ.size() > 0; } );

		Data *pData = m_emptyQ.front();
		m_emptyQ.pop();
		return pData;
	}

	void PushEmptyData(Data *pData) {
		std::unique_lock<std::mutex> lck(m_mtxEmpty);
		m_emptyQ.push(pData);
		m_condEmpty.notify_one();
	}

	Data* PopFullData() {
		std::unique_lock<std::mutex> lck(m_mtxFull);
		m_condFull.wait(lck, [this] { return m_fullQ.size() > 0; });

		Data *pData = m_fullQ.front();
		m_fullQ.pop();
		return pData;
	}

	void PushFullData(Data *pData) {
		std::unique_lock<std::mutex> lck(m_mtxFull);
		m_fullQ.push(pData);
		m_condFull.notify_one();
	}

	~DataQ()
	{
		while (!m_emptyQ.empty())
		{
			DeleteData(m_emptyQ.front());
			m_emptyQ.pop();
		}
		while (!m_fullQ.empty())
		{
			DeleteData(m_fullQ.front());
			m_fullQ.pop();
		}
	}

private:
	std::queue<Data*> m_emptyQ;
	std::queue<Data*> m_fullQ;
	int m_bufferNumber;
	int m_minibachSize;

	std::mutex m_mtxEmpty;
	std::condition_variable m_condEmpty;
	std::mutex m_mtxFull;
	std::condition_variable m_condFull;

};
//extern volatile int64_t coordinating_timestamp;
struct WorkerLRHandle {
	WorkerLRHandle(std::shared_ptr<bool> is_ready_): is_ready(is_ready_){}
	void operator()(const SimpleData& req, SimpleApp* app) {
		if (req.head == MSG_INFORM && req.body == "Worker.Start") {
			std::cout << "Worker-" << MyRank() << "receive Start message scheduler-" << Postoffice::Get()->IDtoRank(req.sender) << std::endl;
			*is_ready = true;
		} else if (req.head == COORDINATE_INFORM) {
			//coordinating_timestamp = stoll(req.body);
		}
		app->Response(req);
	}
	std::shared_ptr<bool> is_ready;
};

// KVServer
template <typename Val>
struct KVServerLRHandle {
	void operator() (const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
		size_t n = req_data.keys.size();
		KVPairs<Val> res;
		if(req_meta.push) { // check if keys.size eq vals.size
			CHECK_EQ(n, req_data.vals.size());
		} else {
			res.keys = req_data.keys;
			res.vals.resize(n);
		}

		// process
		for (size_t i = 0; i < n; ++i) {
			Key key = req_data.keys[i];
			if (req_meta.push) {
				store[key] += req_data.vals[i];
			} else {
				res.vals[i] = store[key];
			}
		}

		// response
		server->Response(req_meta, res);
	}

private:
	std::unordered_map<Key, Val> store;
};

// LRWorker
class LRWorker {
public:
	LRWorker(int app_id = 0) : kv(app_id){
		is_ready = std::make_shared<bool>(false);
		handle = std::make_shared<WorkerLRHandle>(is_ready);
		kv.set_request_handle(*handle);
	}

	bool IsReady() {
		return *is_ready;
	}
	void Learner(DataQ *dataQ, Data* pData) {
		int mini_batch_sample_count = GetSampleCount(pData);

		std::vector<Key> unique_keys;
		std::vector<id_value> all_keys;
		std::vector<float> label(mini_batch_sample_count, -1);
		std::vector<float> pred(mini_batch_sample_count, 0);


		ParseData(pData, all_keys, unique_keys, label);

		// sort all keys (features)
		std::sort(all_keys.begin(), all_keys.end(), [](const id_value& lhs, const id_value& rhs){return lhs.id < rhs.id;});

		// sort unique keys (features)
		std::sort(unique_keys.begin(), unique_keys.end());
		unique_keys.erase(std::unique(unique_keys.begin(), unique_keys.end()), unique_keys.end());

		// pull model
		std::vector<float> model;
		kv.Wait(kv.Pull(unique_keys, &model));

		// calculate w * x
		for (size_t j = 0, i = 0; j < all_keys.size();) {
			Key feature_id = all_keys[j].id;
			Key model_id = unique_keys[i];
			if (feature_id == model_id) {
				pred[all_keys[j].sample] += model[i] * all_keys[j].value;
				++j;
			} else {
				++i;
			}
		}

		// calculate residual error
		for (int i = 0; i < mini_batch_sample_count; ++i) {
			if (label[i] < 0) {
				pred[i] = 0;
				continue;
			}

			if (pred[i] <= -MAX_EXP) pred[i] = 0;
			else if (pred[i] >= MAX_EXP) pred[i] = 1;
			else pred[i] = expTable[(int)((pred[i] + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];
			pred[i] = pred[i] - label[i];
		}

		// calculate gradient
		size_t keys_size = unique_keys.size();
		std::vector<float> grad(keys_size);
		for (size_t j = 0, i = 0; j < all_keys.size();) {
			Key feature_id = all_keys[j].id;
			Key model_id = unique_keys[i];
			int sample = all_keys[j].sample;
			float value = all_keys[i].value;
			if (feature_id == model_id) {
				grad[i] += value * pred[sample];
				++j;
			} else {
				++i;
			}
		}

		// push model
		kv.Wait(kv.Push(unique_keys, grad));

		dataQ->PushEmptyData(pData);
	}
	~LRWorker() {
		kv.SimpleApp::Wait(kv.SimpleApp::Request(MSG_INFORM, "Worker.Done", kScheduler));
	}
private:
	KVWorker<float> kv;
	std::shared_ptr<bool> is_ready;
	std::shared_ptr<WorkerLRHandle> handle;

};

void StartServer(int app_id) {
	if (!IsServer()) return;
	std::cerr << "Server-" << MyRank() << "(" << ") is ready" << std::endl;

	auto server = new KVServer<float>(0);
	server->set_request_handle(KVServerLRHandle<float>());
	RegisterExitCallback([server](){ delete server; });
}

void RunScheduler() {
	if (!IsScheduler()) return;
}

void RunWorker(int app_id) {
	if (!IsWorker()) return;

	using namespace std::placeholders;
	LRWorker worker(app_id);
	//ExpTable();

	//KVWorker<float> kv(0);

	//init
	//int num = 10;
	//std::vector<Key> keys(num);
	//std::vector<float> vals(num);

	//int rank = MyRank();
	//srand(rank + 7);
	//for (int i = 0; i < num; ++i) {
	//	keys[i] = i;
	//	vals[i] = i * 10;
	//}

	////push
	//int repeat = 5;
	//std::vector<int> ts;
	//for (int i = 0; i < repeat; ++i) {
	//	ts.push_back(kv.Push(keys, vals));

	//	// to avoid too frequency push, which leads huge memory usage
	//	if (i > 10) kv.Wait(ts[ts.size() - 10]);
	//}

	//for (int t : ts) {
	//	kv.Wait(t);
	//}

	//// pull
	//std::vector<float> rets;
	//kv.Wait(kv.Pull(keys, &rets));

	//float res = 0;
	//for (int i = 0; i < num; ++i) {
	//	res += fabs(rets[i] - vals[i] * repeat);
	//}
	//CHECK_LT(res / repeat, 1e-5);
	//LL << "error: " << res / repeat;
}


#endif
