#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>

// Page 구조체 정의
struct Page {
    void* header;       // 페이지 헤더
    char** records;     // 레코드 배열
    int numRecords;     // 레코드 개수

    Page(int numRecords) : numRecords(numRecords) {
        header = nullptr;  // 헤더는 일단 nullptr로 초기화
        records = new char*[numRecords];
        for (int i = 0; i < numRecords; ++i) {
            records[i] = nullptr;
        }
    }

    ~Page() {
        for (int i = 0; i < numRecords; ++i) {
            delete[] records[i];
        }
        delete[] records;
    }
};

// Table 구조체 정의
struct Table {
    std::string name;                  // 테이블 이름
    std::vector<Page*> pages;          // 테이블이 보유한 페이지들
    int numRecordsPerPage;             // 페이지당 레코드 수

    Table(const std::string& tableName, int recordsPerPage)
        : name(tableName), numRecordsPerPage(recordsPerPage) {}

    ~Table() {
        for (Page* page : pages) {
            delete page;
        }
    }

    // 새로운 페이지를 추가하는 함수
    void addPage() {
        pages.push_back(new Page(numRecordsPerPage));
    }

    // 레코드를 추가하는 함수
    void addRecord(const std::string& record) {
        if (pages.empty() || pages.back()->numRecords == numRecordsPerPage) {
            addPage();
        }

        Page* lastPage = pages.back();
        int recordIndex = lastPage->numRecords++;
        lastPage->records[recordIndex] = new char[record.size() + 1];
        std::strcpy(lastPage->records[recordIndex], record.c_str());
    }
};

// Database 클래스 - 테이블 생성 및 관리
class Database {
    std::unordered_map<std::string, Table*> tables;

public:
    ~Database() {
        for (auto& pair : tables) {
            delete pair.second;
        }
    }

    // CREATE TABLE 명령 처리
    void createTable(const std::string& name, int recordsPerPage) {
        if (tables.find(name) != tables.end()) {
            std::cerr << "Table already exists: " << name << std::endl;
            return;
        }
        tables[name] = new Table(name, recordsPerPage);
        std::cout << "Table created: " << name << std::endl;
    }

    // INSERT INTO 명령 처리
    void insertInto(const std::string& tableName, const std::string& record) {
        auto it = tables.find(tableName);
        if (it == tables.end()) {
            std::cerr << "Table not found: " << tableName << std::endl;
            return;
        }

        it->second->addRecord(record);
        std::cout << "Inserted record into " << tableName << ": " << record << std::endl;
    }

    Table* getTable(const std::string& name) {
        auto it = tables.find(name);
        if (it != tables.end()) {
            return it->second;
        }
        return nullptr;
    }
};

// 디스크에서 페이지를 읽어오는 역할
class DiskManager {
public:
    Page* getPage(Table* table, int pageNum) {
        if (pageNum < table->pages.size()) {
            return table->pages[pageNum];
        }
        return nullptr;
    }
};

// 연산자 인터페이스
class Operator {
public:
    virtual char* Next() = 0; // 각 연산자는 Next()를 통해 데이터를 반환
    virtual ~Operator() {} // 예외를 던지지 않음
};

// 테이블의 모든 레코드를 순차적으로 읽기
class TableScanOperator : public Operator {
    DiskManager* diskManager;
    Table* table;
    int currentRecordIndex;
    int currentPageNum;
    Page* currentPage;

public:
    TableScanOperator(DiskManager* dm, Table* tbl) 
        : diskManager(dm), table(tbl), currentRecordIndex(0), currentPageNum(0), currentPage(nullptr) {
        currentPage = diskManager->getPage(table, currentPageNum);
    }

    char* Next() {
        while (currentPage == nullptr || currentRecordIndex >= currentPage->numRecords) {
            ++currentPageNum;
            if (currentPage != nullptr) {
                delete currentPage; // 이전 페이지 메모리 해제
            }
            currentPage = diskManager->getPage(table, currentPageNum);

            if (currentPage == nullptr) {
                return nullptr;
            }

            currentRecordIndex = 0;
        }

        return currentPage->records[currentRecordIndex++];
    }

    ~TableScanOperator() { 
        if (currentPage != nullptr) {
            delete currentPage;
        }
    }
};

// 익스큐션 엔진 클래스 정의
class ExecutionEngine {
    Database* db;
    DiskManager* diskManager;

public:
    ExecutionEngine(Database* database, DiskManager* dm) : db(database), diskManager(dm) {}

    // CREATE TABLE 실행 함수
    void executeCreateTable(const std::string& tableName, int recordsPerPage) {
        db->createTable(tableName, recordsPerPage);
    }

    // INSERT INTO 실행 함수
    void executeInsertInto(const std::string& tableName, const std::string& record) {
        db->insertInto(tableName, record);
    }

    // SELECT 실행 함수
    void executeSelect(const std::string& tableName) {
        Table* table = db->getTable(tableName);
        if (table == nullptr) {
            std::cerr << "Table not found: " << tableName << std::endl;
            return;
        }

        TableScanOperator tableScan(diskManager, table);
        char* record;
        while ((record = tableScan.Next()) != nullptr) {
            std::cout << "Record: " << record << std::endl;
        }
    }
};

// 메인 함수
int main() {
    Database db;
    DiskManager dm;
    ExecutionEngine engine(&db, &dm);

    // 쿼리 시뮬레이션
    std::string createTableQuery = "CREATE TABLE Students 10";
    std::string insertQuery1 = "INSERT INTO Students VALUES('jaehong, 1')";
    std::string insertQuery2 = "INSERT INTO Students VALUES('jingyeong, 2')";
    std::string selectQuery = "SELECT * FROM Students";

    // 쿼리 파서 역할을 하는 부분 
    // CREATE TABLE test
    engine.executeCreateTable("Students", 10);

    // INSERT INTO test
    engine.executeInsertInto("Students", "jaehong, 1");
    engine.executeInsertInto("Students", "jingyeong, 2");

    // SELECT test
    engine.executeSelect("Students");

    return 0;
}
