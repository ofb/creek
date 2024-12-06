#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <cmath>
#include <sstream>
#include <string>
#include <fstream>
#include <stdexcept> // std::runtime_error
#include <chrono>
using namespace std;
using namespace std::chrono;

// https://www.gormanalysis.com/blog/reading-and-writing-csv-files-with-cpp/

unsigned int get_rows()
{
	ifstream myFile("/mnt/disks/creek-1/pearson/pearson.config");
	if (!myFile.is_open()) throw runtime_error("Could not open interpolated.csv");
	string line;
	if (myFile.good()) getline(myFile, line);
	else throw runtime_error("config stream is not good");
	return static_cast<unsigned int>(stoi(line));
}

vector<string> get_symbols()
{
	vector<string> stocks;
	ifstream myFile("/mnt/disks/creek-1/pearson/interpolated.csv");
	if (!myFile.is_open()) throw runtime_error("Could not open interpolated.csv");
	vector<string> row;
	string line, word;
	if (myFile.good()) getline(myFile, line);
	else throw runtime_error("interpolated.csv stream is not good");
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

void get_data(const string& symbol, float* database, unsigned int databaseRows) {
	string path = "/mnt/disks/creek-2/us_equities_interpolated/";
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

float pearson(const float* x, const float* y, unsigned int databaseRows) {
	double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0;
	double squaresum_x = 0.0, squaresum_y = 0.0;
	for (unsigned int i = 0; i < databaseRows; i++) {
		sum_x += x[i];
		sum_y += y[i];
		sum_xy += x[i] * y[i];
		squaresum_x += x[i] * x[i];
		squaresum_y += y[i] * y[i];
	}
	return static_cast<float>((databaseRows * sum_xy - sum_x * sum_y)
						/ sqrt((databaseRows * squaresum_x- sum_x * sum_x)
							* (databaseRows * squaresum_y - sum_y * sum_y ))
							);
}

void writeResults(float** results, vector<string>& symbols, const int N) {
	ofstream myFile("/mnt/disks/creek-1/pearson/pearson.csv");
	myFile << "symbol1,symbol2,pearson" << endl;
	for (int i = 0; i < N; i++) {
		for (int j = 0; j < N-i-1; j++) {
			myFile << symbols[i] << "," << symbols[i+j+1] 
				<< "," << results[i][j] << endl;
		}
	}
	myFile.close();
	return;
}

int main ()
{
	unsigned int databaseRows = get_rows();
	// Get starting timepoint
	auto load_start = high_resolution_clock::now();
	vector<string> symbols;
	symbols = get_symbols();
	const int N = static_cast<int>(symbols.size());
	cout << "N=" << N << endl;
	// Allocate our database on the heap
	float** database;
	database = new float*[N];
	for (int i = 0; i < N; i++) {
		database[i] = new float[databaseRows];
		get_data(symbols[i], database[i], databaseRows);
	}
	// The following 2D array will store our results
	float** results;
	results = new float*[N];
	for (int i = 0; i < N; i++) {
		results[i] = new float[N-i-1];
	}
	// Get ending timepoint
	auto load_stop = high_resolution_clock::now();
	auto duration = duration_cast<microseconds>(load_stop - load_start);
	cout << "Time taken to load databases: "
			<< duration.count() << " microseconds" << endl;
	
	
	
	// Get starting timepoint
	auto start = high_resolution_clock::now();

	std::pair<size_t,size_t> pairs;
	pairs.reserve(N * (N + 1) / 2);
	for (size_t i = 0; i < N; i++) {
		for (size_t j = 0; j < N-i-1; j++) {
			pairs.emplace_back(i,j));
		}
	}
	
	// must complile with openmp: g++ -fopenmp pearson.cpp
	#pragma omp parallel for schedule (static,10)
	for (size_t i = 0; i < pairs.size(); i++) {
		auto& p = pairs[i]; 
		results[p.first][p.second] = pearson(database[p.first], database[p.first + p.second + 1], databaseRows);
	}
	// Get ending timepoint
	auto stop = high_resolution_clock::now();
	
	// Get duration. Substart timepoints to
	// get duration. To cast it to proper unit
	// use duration cast method
	duration = duration_cast<microseconds>(stop - start);
	cout << "Time taken by correlation function: "
			<< duration.count() << " microseconds" << endl;
	
	writeResults(results, symbols, N);
	
	
	for (int i = 0; i < N; i++) {
		delete[] database[i];
		delete[] results[i];
	}
	delete[] database;
	delete[] results;
	return 0;
}
