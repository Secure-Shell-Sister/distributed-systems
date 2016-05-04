#include <iostream>
#include <string>
#include <regex>
#include <fstream>
#include <map>
#include <vector>
#include <algorithm>

#include "rapidjson/writer.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

#include <mpi.h>
#include <dirent.h>
#include <stdio.h>

void parse_log(const char*, size_t, std::map<std::string, int>*);
int main(int argc, char** argv){
	int myid, numprocs;
	int comm_tag = 2768;

	MPI_Status status;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);

	if(myid == 0){

		DIR *dir;
		struct dirent *ent;
		std::string log_path("./var/log/"); //Location of the log files
		std::vector<std::string> file_list;

		//List all log file with 'secure' in its name
		std::cout << "Listing all files\n";
		if((dir = opendir(log_path.c_str())) != NULL){
			while((ent = readdir(dir)) != NULL){
				if(strstr(ent->d_name, "secure") != NULL){
					file_list.push_back(log_path + std::string(ent->d_name));
				}
			}
		}
		closedir(dir);

		//Broadcast the total number of logs to the worker
		std::cout << "Broadcasting total number of logs\n";
		int total_files = file_list.size();
		std::cout << "Total job size: " << total_files << "\n";
		MPI_Bcast(&total_files, 1, MPI_INT, 0, MPI_COMM_WORLD);

		//Make job list for every worker
		std::vector<int> workloads(file_list.size());
		std::vector<int> worker_jobs(numprocs, 0);
		int work_size = file_list.size() / numprocs;
		int work_overflow = file_list.size() % numprocs;

		std::cout << "Dividing the job among workers\n";
		int job_counter = 0;
		for(int i = 0; i < numprocs; ++i){
			int job_size = (i < work_overflow) ? work_size + 1 : work_size;
			worker_jobs[i] = job_size;
			for(int j = 0; j < job_size; ++j){
				workloads[job_counter++] = i;
			}
		}

		//Log content container for master
		int master_job = (work_overflow > 0) ? work_size + 1 : work_size;
		std::cout << "Master job size: " << master_job << "\n";

		char **master_log_container = new char*[master_job];
		size_t *master_content_size = new size_t[master_job];
		//-----------------------------------------------------

		//Send the log content to designated worker
		std::cout << "Sending logs to the workers\n";
		job_counter = 0;
		for(int i = 0; i < file_list.size(); ++i){
			std::ifstream log_file(file_list[i].c_str());
			log_file.seekg(0, std::ios::end);
			size_t file_size = log_file.tellg();
			log_file.seekg(0);

			char* log_content = new char[file_size];
			log_file.read(log_content, file_size);

			if(workloads[i] == 0){
				master_log_container[job_counter] = log_content;
				master_content_size[job_counter] = file_size;
				++job_counter;
			}
			else{
				MPI_Send(&file_size, 1, MPI_INT, workloads[i], comm_tag, MPI_COMM_WORLD);
				//Skipping empty file to avoid SIGSEV
				if(file_size > 0)
					MPI_Send(log_content, file_size, MPI_CHAR, workloads[i], comm_tag, MPI_COMM_WORLD);

				delete[] log_content;
			}
		}

		std::cout << "Parsing the logs\n";
		std::map<std::string, int> tag_counter;
		for(int i = 0; i < master_job; ++i){
			if(master_content_size[i] <= 0){
				//std::cout << file_list[i] << "\n";
				continue;
			}
			parse_log(master_log_container[i], master_content_size[i], &tag_counter);
			delete[] master_log_container[i];
		}

		//Free the memory
		std::cout << "Freeing memories\n";
		//for(int i = 0; i < master_job; ++i) delete[] master_log_container[i];
		delete[] master_log_container;
		delete[] master_content_size;
		//--------------------------------------------

		std::cout << "Receiving results from workers\n";
		for(int i = 1; i < numprocs; ++i){
			if(worker_jobs[i] == 0) break;
			int message_size;
			rapidjson::Document document;
			char *json_message;

			//Receive json result from workers
			MPI_Recv(&message_size, 1, MPI_INT, i, comm_tag, MPI_COMM_WORLD, &status);
			json_message = new char[message_size];
			MPI_Recv(json_message, message_size, MPI_CHAR, i, comm_tag, MPI_COMM_WORLD, &status);
			json_message[message_size - 1] = '\0';
			//std::cout << json_message << "\n\n";

			//Parse json message
			document.Parse(json_message);

			//Merge the map
			for(auto it = document.MemberBegin(); it != document.MemberEnd(); ++it){
				auto key_string = it->name.GetString();
				int value = it->value.GetUint();
				if(tag_counter.count(key_string)) tag_counter[key_string] += value;
				else tag_counter.insert(std::pair<std::string, int>(key_string, value));
			}

			//Free the memory
			delete[] json_message;
		}

		//Sort and display the result
		std::cout << "Sorting the result\n";
		std::vector<std::pair<std::string, int> > map_v;
		for(auto it = tag_counter.begin(); it != tag_counter.end(); ++it) map_v.push_back(*it); //Copy by value
		auto sort_function = [](std::pair<std::string, int>& a, std::pair<std::string, int>& b){return a.second > b.second;};
		std::sort(map_v.begin(), map_v.end(), sort_function);

		std::cout << "Operation finished!\n\n";
		for(int i = 0; i < map_v.size(); ++i){
			printf("%s => %d\n", map_v[i].first.c_str(), map_v[i].second);
		}

	}
	else{
		//Receive the total number of logs from the master
		int total_logs;
		MPI_Bcast(&total_logs, 1, MPI_INT, 0, MPI_COMM_WORLD);

		int work_size = total_logs / numprocs;
		int job_size = (myid < (total_logs % numprocs)) ? work_size + 1 : work_size;
		//std::cout << "Worker " << myid << " job size: " << job_size << "\n";

		//Variable to contain the log_content
		char** log_container = new char*[job_size];
		int* log_file_size = new int[job_size];

		//Receive the log_content from master
		//std::cout << "Worker " << myid << " receiving logs\n";
		for(int i = 0; i < job_size; ++i){
			MPI_Recv(&log_file_size[i], 1, MPI_INT, 0, comm_tag, MPI_COMM_WORLD, &status);

			//Skipping empty file to avoid SIGSEV
			if(log_file_size[i] > 0){
				log_container[i] = new char[log_file_size[i]];
				MPI_Recv(log_container[i], log_file_size[i], MPI_CHAR, 0, comm_tag, MPI_COMM_WORLD, &status);
				log_container[i][log_file_size[i] - 1] = '\0';
			}
		}

		//Parse the logs
		//std::cout << "Worker " << myid << " parsing logs\n";
		std::map<std::string, int> tag_counter;
		for(int i = 0; i < job_size; ++i){
			if(log_file_size[i] > 0){
				parse_log(log_container[i], log_file_size[i], &tag_counter);
				delete[] log_container[i];
			}
		}

		//Free the memory
		delete[] log_container;
		delete[] log_file_size;
		//------------------------

		//Serialize to JSON
		//std::cout << "Worker " << myid << " serializing result to JSON\n";
		rapidjson::StringBuffer s;
		rapidjson::Writer<rapidjson::StringBuffer> writer(s);

		writer.StartObject();
		for(auto it = tag_counter.begin(); it != tag_counter.end(); ++it){
			//std::cout << it->first << " => " << it->second << '\n';

			writer.String(it->first.c_str());
			writer.Uint(it->second);
		}
		writer.EndObject();

		//Send back the result to master
		//std::cout << "Worker " << myid << " sending JSON to master\n";
		//std::cout << s.GetString() << "\n";
		int message_size = s.GetSize() + 1;
		void* json_message = const_cast<char*>(s.GetString());
		MPI_Send(&message_size, 1, MPI_INT, 0, comm_tag, MPI_COMM_WORLD);
		MPI_Send(json_message, message_size, MPI_CHAR, 0, comm_tag, MPI_COMM_WORLD);
	}

	MPI_Finalize();
	return 0;
}

void parse_log(const char *log_content, size_t file_size, std::map<std::string, int> *tag_counter){

	std::regex re("\\[[[:d:]]+\\]: (.+?) (for|from)", std::regex_constants::ECMAScript);


	auto word_begin = std::cregex_iterator(log_content, log_content + file_size - 1, re);
	auto word_end = std::cregex_iterator();


	for(auto i = word_begin; i != word_end; ++i){
		//std::cout << "word" << std::endl;
		std::cmatch match = *i;
		std::string key_string;
		if(match[2] == "for"){
			key_string = match[1].str();
		}
		else{
			std::string current_string = match[1].str();
			size_t last_space = current_string.find_last_of(' ');
			if(last_space != std::string::npos){
				key_string = current_string.substr(0, last_space);
			}
			else{
				key_string = current_string;
			}
		}
		//for(auto x:match) std::cout << x << std::endl;
		if(tag_counter->count(key_string))
			++(*tag_counter)[key_string];
		else
			tag_counter->insert(std::pair<std::string, int>(key_string, 1));
	}
}
