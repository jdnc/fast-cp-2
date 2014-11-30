#include <iostream>
#include <vector>
#include <sstream>
#include <cstring>

std::string trimWhiteSpaces(std::string str){
	std::string whitespaces(" \t\f\v\n\r");
    std::size_t found = str.find_first_not_of(whitespaces);
    if(found != std::string::npos)
    	str = str.erase(0,found);
    found = str.find_last_not_of(whitespaces);
    if(found != std::string::npos)
    	str = str.erase(found+1);
	return str;
}

std::vector<std::string> splitStringByDelimiter(std::string str,char delim){
	std::vector<std::string> vecTemp;
    std::istringstream partialString(str);
    std::string tempPartialLine;
    while(getline(partialString,tempPartialLine,delim)){
    	vecTemp.push_back(tempPartialLine);
    }
    return vecTemp;
}

