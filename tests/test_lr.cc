#include "ps/ps.h"

using namespace ps;

int ArgPos(const std::string &str, int argc, const char * const * argv) {
	int a;
	for (a = 1; a < argc; a++) if (str == argv[a]) {
		if (a == argc - 1){
			printf("Argument missing for %s\n", str.c_str());
			exit(1);
		}
		return a;
	}
	return -1;
}

int main(int argc, char *argv[]) {
	int i;
	int app_id = 0;

	if ((i = ArgPos((char *)"-app-id", argc, argv)) > 0) app_id = atoi(argv[i + 1]);

	std::cout<<app_id << std::endl;
	return 0;
}


//namespace distlr {
//	LR::LR(int num_feature_dim, float learning_rate, float C, int random_state) : num_feature_dim_(num_feature_dim), learning_rate_(learning_rate), C_(C), random_state_(random_state) {
//			InitWeight_();
//	}
//
//	void LR::SetKVWorker(ps::KVWorker<float>* kv) {
//			kv_ = kv;
//	}
//
//	void LR::Train(DataIter& iter, int batch_size = 100) {
//			while (iter.HasNext()) {
//					std::vector<Sample> batch = iter.NextBatch(batch_size);
//					PullWeight_();
//
//					std::vector<float> grad(weight_.size());
//			}
//	}
//}
