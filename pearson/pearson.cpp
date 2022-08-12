#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <array>
#include <sstream>
#include <string>
#include <fstream>
#include <stdexcept> // std::runtime_error
using namespace std;

// Number of stocks with nonempty data = 5246
// Number of rows in interpolated database for each stock = 308491
unsigned int databaseRows = 308491;
// https://www.geeksforgeeks.org/csv-file-management-using-c/

vector<string> get_symbols()
{
	vector<string> stocks;
	ifstream myFile("nonempty_shortable_equity_list.csv");
	if (!myFile.is_open()) throw runtime_error("Could not open file");
	vector<string> row;
	string line, word;
	if (myFile.good()) getline(myFile, line);
	else throw runtime_error("nonempty_shortable_equity_list.csv stream is not good");
	while (getline(myFile, line))
	{
		row.clear();
		stringstream ss(line);
		while (getline(ss, word, ',')) {
			row.push_back(word);
		}
		if (row.size() > 1) stocks.push_back(row[1]);
	}
	return stocks;
}

void get_data(const string& symbol, float* database) {
	string path = "/mnt/disks/creek-1/us_equities_2022_interpolated/";
	path = path + symbol + ".csv";
	ifstream myFile(path);
	if (!myFile.is_open()) throw runtime_error("Could not open database");
	vector<string> row;
	unsigned int counter = 0;
	string line, word;
	if (myFile.good()) getline(myFile, line);
	else throw runtime_error("database stream is not good");
	while (getline(myFile, line))
	{
		row.clear();
		stringstream ss(line);
		while (getline(ss, word, ',')) {
			row.push_back(word);
		}
		if (row.size() > 1 && counter < databaseRows) database[counter++] = stof(row[1]);
		else throw runtime_error("Row too short or counter too big");
	}
	if (counter != databaseRows) {
		cout << "Counter on " << symbol << " is " << counter << endl;
		throw runtime_error("Database wrong size");
	}
	return;
}


int main ()
{
	vector<string> symbols;
	symbols = get_symbols();
	const int N = static_cast<int>(symbols.size());
	cout << "N=" << N << endl;
	// Allocate our database on the heap
	float** database;
	database = new float*[N];
	for (int i = 0; i < N; i++) {
		database[i] = new float[databaseRows];
		get_data(symbols[i], database[i]);
		cout << "Symbol " << symbols[i] << ": " << endl;
		for (int j = 0; j < 10; j++) {
			cout << database[i][j] << endl;
		}
	}
	for (int i = 0; i < N; i++) delete[] database[i];
	delete[] database;
	return 0;
}