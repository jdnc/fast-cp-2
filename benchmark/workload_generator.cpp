#include <iostream>
#include <vector>
#include <algorithm>
#include "stringops.h"
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

void createFile(std::string full_path,int nb_bytes);
void createDirectory(std::string full_path);

struct cmd_line_args{
private:
    std::vector<std::string> params;
    int recursive_depth;
    std::string dir;
    int nb_bytes;
	int nb_files;
public:
    cmd_line_args():recursive_depth(-1),dir(""),nb_bytes(-1),nb_files(1){}
    cmd_line_args(std::vector<std::string> params){
		this->params = params;
		std::string recdepth = getValue("-rd");
		if(recdepth == "")
			recursive_depth = 0;
		else
        	recursive_depth = atoi(recdepth.c_str());
        dir = getValue("-dir");
        nb_bytes = atoi(getValue("-nb").c_str());
		std::string nfiles = getValue("-nf");
		if(nfiles == "")
			nb_files = 1;
		else
			nb_files = atoi(nfiles.c_str());
    }
    int getNbBytes(){
        return nb_bytes;
    }
    std::string getDir(){
        return dir;
    }
    int getRecursiveDepth(){
        return recursive_depth;
    }
    std::string getValue(std::string arg){
        int pos = std::find(params.begin(),params.end(),arg) - params.begin();
        std::string result = "";
        if(pos != (params.end() - params.begin())){
            for(int j=pos+1;j<params.size();j++){
                if(params[j][0] == '-')
                    break;
                result += " " + params[j];
            }
        }
        result = trimWhiteSpaces(result);
        return result;
    }
	int getNbFiles(){
		return nb_files;
	}
};


int main(int argc,char* argv[]){

	if(argc < 4){
		std::cout << std::endl << "Enter all arguments:" 
												<< std::endl;
		return -1;
	}

	std::vector<std::string> params(argv,argc+argv);
	cmd_line_args cargs(params);
	
	if(cargs.getNbBytes() == -1){
		std::cout << std::endl << "Enter number of bytes" 
												<< std::endl;
		return -1;
	}

	if(cargs.getDir().empty()){
		std::cout << std::endl << "Enter directory" << std::endl;
		return -1;
	}

	if(cargs.getRecursiveDepth() == -1){
		std::cout << std::endl << "Enter recursive depth" 
													<< std::endl;
		return -1;
	}

	std::string full_name = std::string(cargs.getDir());

   	for(int j=0;j<cargs.getRecursiveDepth();j++){
   		std::string name = full_name + "/temp.XXXXXX";
        char* new_name = strdup(name.c_str());
        char* dirPtr = mkdtemp(new_name);
        full_name = std::string(dirPtr);
    }

	for(int i=0;i<cargs.getNbFiles();i++){
		std::string full_path = full_name + "/file" + std::to_string(i);		
		int fd = open(full_path.c_str(),O_CREAT|O_WRONLY,S_IRWXU);
	    int ret = ftruncate(fd,cargs.getNbBytes());
		close(fd);
	}
	
	std::cout << std::endl;
	return 0;
}
