#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>

// Page 구조체 정의
struct Page {
    void* header;       // 페이지 헤더
    char** records;     // 레코드 배열
    int numRecords;     // 레코드 개수

    // 생성자
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
    Table* table;

public:
    DiskManager(Table* table) : table(table) {}

    Page* getPage(int pageNum) {
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
    virtual ~Operator() noexcept = default; // 예외를 던지지 않음
};

// 테이블의 모든 레코드를 순차적으로 읽기
class TableScanOperator : public Operator {
    DiskManager* diskManager;
    int currentRecordIndex;
    int currentPageNum;
    Page* currentPage;

public:
    TableScanOperator(DiskManager* dm) 
        : diskManager(dm), currentRecordIndex(0), currentPageNum(0), currentPage(nullptr) {
        currentPage = diskManager->getPage(currentPageNum);
    }

    char* Next() override {
        if (currentPage == nullptr) return nullptr;

        if (currentRecordIndex < currentPage->numRecords) {
            return currentPage->records[currentRecordIndex++];
        } else {
            // 다음 페이지로 넘어감
            ++currentPageNum;
            delete currentPage; // 이전 페이지 메모리 해제
            currentPage = diskManager->getPage(currentPageNum);

            if (currentPage == nullptr) return nullptr;

            currentRecordIndex = 0;
            return Next();
        }
    }

    ~TableScanOperator() noexcept override { // 예외를 던지지 않음
        if (currentPage != nullptr) {
            delete currentPage;
        }
    }
};

//  AST를 받아 쿼리 실행
class ExecutionEngine {
    Operator* rootOperator;

public:
    ExecutionEngine(Operator* rootOp) : rootOperator(rootOp) {}

    void execute() {
        char* record;
        while ((record = rootOperator->Next()) != nullptr) {
            std::cout << "Record: " << record << std::endl;
        }
    }
};

//  AST를 받아 실행 엔진을 실행
int main() {
    Database db;

    // CREATE TABLE 예제
    db.createTable("Students", 10); // 테이블 이름 Students, 페이지당 레코드 수 10

    // INSERT INTO 예제
    db.insertInto("Students", "jaehong, 1");
    db.insertInto("Students", "jingyeong, 2");
    db.insertInto("Students", "binwon, 3");
    db.insertInto("Students", "jeonghwan, 4");

    // 테이블 스캔 및 쿼리 실행 예제
    Table* usersTable = db.getTable("Users");
    if (usersTable) {
        DiskManager dm(usersTable);
        TableScanOperator tableScan(&dm);
        ExecutionEngine engine(&tableScan);
        engine.execute(); // "Users" 테이블의 모든 레코드를 출력
    }

    return 0;
}
