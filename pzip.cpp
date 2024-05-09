// Hayate Saito
// pwzip.cpp

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h> // for mmap
#include <sys/stat.h> // for stat
#include <sys/sysinfo.h> // for get_nprocs_conf
#include <pthread.h>
#include <deque>
#include <cstring>
#include <cstdlib> // for exit()

using namespace std;

// This is for the arguments of readData
struct readArgs{
    size_t fileSize;
    int threadID;
    char* content;
    size_t chunkSize;
};

// This is for the arguments of compressData
struct compressArgs{
    int threadID;
    string content;
    int NUM_THREADS;
};

// This holds the information about the mapped file
struct MappedFileContent{
    char *content;
    size_t fileSize;
};


deque<char> buffer; // This stores the data from the mapped file
deque<char> compressBuffer; // This stores the data that consuming threads bring from the shared buffer that has the data of mapped file
int compressCount = 1; // This counts the number of consecutive characters

int total_procs = get_nprocs_conf(); // This gets the number of available processors

// Initialization for providing threads synchronization
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_read = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_compress = PTHREAD_COND_INITIALIZER;
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;

// These are to control the execution order
int read_next_thread = 0;
int compress_next_thread = 0;


void* readData(void* arg);
void* compressData(void* arg);


// This function opens and maps the file
MappedFileContent readFileWithMap(const string &filename) {

    // Open the file
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        cout << "Error opening file" << endl;
    }

    // Stat is to hold tje information about the file
    // retrieve information about the file and store it in sb
    struct stat sb;

    if (fstat(fd, &sb) == -1) {
        close(fd);
        cout << "Failed to retrieve information" << endl;
    }

    // Getting the size of mapped file
    off_t fileSize = sb.st_size;

    // Map the file and save the content about it
    void *mmap_return = mmap(nullptr, fileSize, PROT_READ, MAP_SHARED, fd, 0);
    char *fileContent = (char *) mmap_return;  // Casting void pointer to char p$

    // Save the information about the mapped file into struct
    MappedFileContent mappedFileContent;
    mappedFileContent.content = fileContent;
    mappedFileContent.fileSize = static_cast<size_t>(sb.st_size); // cast st_si$

    return mappedFileContent;
}


int main(int argc, char* argv[]){

    if (argc != 2){
        cout << "Usage: " << argv[0] << " <filename>" << endl;
        exit(-1);
    }
    string fileName = argv[1];

    // Get the information about the mapped file
    MappedFileContent fileContent = readFileWithMap(fileName);

    // Determine the number of threads based on the size of the mapped file
    // If the size of the file is bigger than the number of available processor, the number of threads will be same as file size otherwise the number of processors
    int NUM_THREADS;
    if(fileContent.fileSize <= (size_t)(total_procs)){
        NUM_THREADS = (int)(fileContent.fileSize);
    }else{
        NUM_THREADS = total_procs;
    }

    // Save the information about producing threads
    pthread_t read_id[NUM_THREADS];
    int read_rc[NUM_THREADS];
    readArgs read_args[NUM_THREADS];

    // Save the information about consuming threads
    pthread_t compress_id[NUM_THREADS];
    int compress_rc[NUM_THREADS];
    compressArgs compress_args[NUM_THREADS];


    // Creating Threads for reading data from mapped file
    for(int i = 0; i < NUM_THREADS; i++){

        // Saving the information needed to execute readFile
        read_args[i].fileSize = fileContent.fileSize;
        read_args[i].threadID = i;
        read_args[i].content = fileContent.content;

        // Deciding the size of value that each thread deals with
        if(fileContent.fileSize < (size_t)(total_procs)){
            read_args[i].chunkSize = 1;
        }else{
            // In this case, we increment the size of value otherwise, there will be memory not gotten by threads
            if(fileContent.fileSize % total_procs != 0){
                read_args[i].chunkSize = (fileContent.fileSize / total_procs) + 1;
            }else{
                read_args[i].chunkSize = fileContent.fileSize / total_procs;
            }
        }

        // Creating the thread for reading data
        read_rc[i] = pthread_create(&read_id[i], nullptr, readData, &read_args[i]);

        if(read_rc[i]) {
            cout << "Failed to crate thread" << endl;
            exit(-1);
        }
    }

    // Wait for all threads to finish                                          $
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(read_id[i], nullptr);
    }



    // Creating Threads for compressing data from mapped file
    for(int i = 0; i < NUM_THREADS; i++){

        // Saving the information needed to execute compressFile
        compress_args[i].threadID = i;
        compress_args[i].NUM_THREADS = NUM_THREADS;
        string content;

        if(fileContent.fileSize < (size_t)(total_procs)){
            content = buffer.front();
            buffer.pop_front();
        }else{
            size_t size;
            if(fileContent.fileSize % total_procs != 0){
                size  = (fileContent.fileSize / total_procs) + 1;
            }else{
                size = fileContent.fileSize / total_procs;
            }
            for(size_t l = 0; l < size; l++){
                content += buffer.front();
                buffer.pop_front();
            }
        }

        compress_args[i].content = content;

        // Creating the consuming threads that compress the data
        compress_rc[i] = pthread_create(&compress_id[i], nullptr, compressData, &compress_args[i]);
        if(compress_rc[i]){
            cout << "Failed to crate thread" << endl;
            exit(-1);
        }
    }

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(compress_id[i], nullptr);
    }

    // Adding the newline character at the end
    cout.write((char*)"\n", sizeof(char));

    return 0;

}

void* readData(void* arg) {

    // Saving the information needed to execute readFile
    readArgs *threadArgs = (readArgs *) arg;
    int threadID = threadArgs->threadID;
    char *content = threadArgs->content;

    size_t dataChunk = threadArgs->chunkSize; // The size of data that one thread

    // Lock mutex before accessing shared resource
    pthread_mutex_lock(&buffer_mutex);

    // Wait till the thread gets its turn to execute
    while (threadID != read_next_thread) {
        pthread_cond_wait(&cond_read, &buffer_mutex);
    }

    for (size_t i = 0; i < dataChunk; i++) {
        buffer.push_back(content[i + threadID * dataChunk]);
    }

    read_next_thread++;

    // signal other thread to check if it is their turn to execute
    pthread_cond_broadcast(&cond_read); // This can't be signal because signal $

    // signal that the buffer is not empty anymore
    pthread_cond_signal(&not_empty);

    // Unlock mutex after accessing shared resource
    pthread_mutex_unlock(&buffer_mutex);

    return nullptr;

}

void* compressData(void* arg){

    // Saving the information needed to execute compressFile
    compressArgs* threadArgs = (compressArgs*)arg;
    string content = threadArgs->content; // Assuming content is a std::string
    const char* value = content.c_str();
    int threadID = threadArgs->threadID;
    int length = strlen(value);

    // Lock mutex before accessing shared resource
    pthread_mutex_lock(&buffer_mutex);

    // Wait till the thread gets its turn to execute
    while(threadID != compress_next_thread){
        pthread_cond_wait(&cond_compress, &buffer_mutex);
    }

    // Compress the data and write it if it is the last element of the consecutive characters
    if(length < 1 && compressCount > 1){
        cout.write((char*)&compressCount, sizeof(int));
        cout.write((char*)&compressBuffer.back(), sizeof(char));
        compressCount = 0;
    }

    // Iterate through the data that one threads have and check if its consecutive
    for(int i = 0; i < length; i++){
        char character = (char)(value[i]);

        // If it is the first character, it saves it
        // If it is the same character as the one saved last, increments the count
        // If it is the character that is not same as the one saved last, write the consecutive characters
        if(compressBuffer.empty()) {
            compressBuffer.push_back(character);
        }else if (character == compressBuffer.back()){
            compressCount++;
            compressBuffer.push_back(character);
        }else{
            cout.write((char*)&compressCount, sizeof(int));
            cout.write((char*)&compressBuffer.back(), sizeof(char));
            compressCount = 1;
            compressBuffer.push_back(character);
        }
    }

    // Increment the number so that the next thread can get in the critical region
    compress_next_thread++;

    // signal other thread to check if it is their turn to execute
    pthread_cond_broadcast(&cond_compress); // This can't be signal because sig$

    // Unlock mutex after accessing shared resource
    pthread_mutex_unlock(&buffer_mutex);

    return nullptr;

}







