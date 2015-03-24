package yakc

import "testing"
import "io/ioutil"
import "os"
import "path"
import "os/exec"
//import "fmt"

type testingInfo struct {
	t *testing.T
	tmpDir string
	kdb *KyotoDB
}

func tearUp(t *testing.T) (info testingInfo) {
	tmpDir, err := ioutil.TempDir("", "yakc-go")
	if err != nil {t.Errorf("Cannot create temporally directory: %s", err)}
	kdb, err := Open(path.Join(tmpDir, "testdb.kch"))
	if err != nil {t.Errorf("Cannot open DB: %s", err)}
	info = testingInfo{t, tmpDir, kdb}
	return
}

func tearUpWithData(t *testing.T) (info testingInfo) {
	tmpDir, err := ioutil.TempDir("", "yakc-go")
	if err != nil {t.Errorf("Cannot create temporally directory: %s", err)}
	tmpFile := path.Join(tmpDir, "testdb.kch")

	err = exec.Command("kchashmgr", "create", tmpFile).Run()
	if err != nil {t.Errorf("Failed to create data (%s)", err)}

	err = exec.Command("kchashmgr", "set", tmpFile, "1", "2").Run()
	if err != nil {t.Errorf("Failed to set data (%s)", err)}
	err = exec.Command("kchashmgr", "set", tmpFile, "A", "B").Run()
	if err != nil {t.Errorf("Failed to set data (%s)", err)}
	err = exec.Command("kchashmgr", "set", tmpFile, "z", "y").Run()
	if err != nil {t.Errorf("Failed to set data (%s)", err)}
	err = exec.Command("kchashmgr", "set", tmpFile, "ABC", "124").Run()
	if err != nil {t.Errorf("Failed to set data (%s)", err)}
	
	kdb, err := Open(tmpFile)
	if err != nil {t.Errorf("Cannot open DB: %s", err)}
	info = testingInfo{t, tmpDir, kdb}
	return
}


func (info *testingInfo) tearDown()  {
	//fmt.Printf("temporary directory: %s\n", info.tmpDir)

	err := info.kdb.Close()
	if err != nil {
		info.t.Errorf("Failed to close %s", err)
	}
	
	os.Remove(path.Join(info.tmpDir, "testdb.kch"))
	os.Remove(info.tmpDir)
}

func TestOpen1(t *testing.T) {
	_, err := Open("/proc/not/exist/file")
	if err == nil {
		t.Errorf("This operation should fail")
	}
}

func TestOpen2(t *testing.T) {
	info := tearUp(t)

	err := info.kdb.Set("AbC", "Hi!")
	if err != nil {t.Errorf("Failed to set value %s", err)}

	v, err := info.kdb.GetOrDefault("OK", "A!")
	if err != nil {t.Errorf("Failed to get value")}
	if v != "A!" {t.Errorf("Invalid value %s", v)}
	
	info.tearDown()
}

func TestOpen3(t *testing.T) {
	info := tearUpWithData(t)
	v, err := info.kdb.Get("1")
	if err != nil {t.Errorf("Failed to get value")}
	if v != "2" {t.Errorf("Invalid value %s", v)}

	v, err = info.kdb.Get("ABC")
	if err != nil {t.Errorf("Failed to get value")}
	if v != "124" {t.Errorf("Invalid value %s", v)}

	info.tearDown()
}

func TestContains(t *testing.T) {
	info := tearUpWithData(t)

	if ! info.kdb.Contains("ABC") {t.Errorf("Failed while testing contains 1")}
	if ! info.kdb.Contains("1") {t.Errorf("Failed while testing contains 2")}
	if info.kdb.Contains("X") {t.Errorf("Failed while testing contains 3")}
	if info.kdb.Contains("abc") {t.Errorf("Failed while testing contains 4")}

	info.tearDown()
}

func TestCount(t *testing.T) {
	info := tearUpWithData(t)

	count, err := info.kdb.Count()
	if err != nil {t.Errorf("Some error %s", err.Error())}
	if count != 4 {t.Errorf("Failed to count")}

	info.kdb.Set("!!!", "@@@");

	count, err = info.kdb.Count()
	if err != nil {t.Errorf("Some error %s", err.Error())}
	if count != 5 {t.Errorf("Failed to count")}

	info.tearDown()
}

func TestAppend(t *testing.T) {
	info := tearUpWithData(t)
	defer info.tearDown()
	
	err := info.kdb.Append("ABC", "568");
	if err != nil {t.Errorf("Cannot append data")}

	v, err := info.kdb.Get("ABC");
	if err != nil {t.Errorf("Cannot get data")}
	if v != "124568" {t.Errorf("Invalid data")}
}

func TestClear(t *testing.T) {
	info := tearUpWithData(t)
	defer info.tearDown()
	
	err := info.kdb.Clear()
	if err != nil {t.Errorf("Cannot append data")}

	c, err := info.kdb.Count()
	if err != nil {t.Errorf("Cannot count data")}
	if c != 0 {t.Errorf("Invalid number of data")}
}

func TestPop(t *testing.T) {
	info := tearUpWithData(t)
	defer info.tearDown()

	ok := info.kdb.Contains("ABC");
	if !ok {t.Errorf("data is not found")}

	v, err := info.kdb.Pop("ABC");
	if err != nil {t.Errorf("Cannot get data %s", err)}
	if v != "124" {t.Errorf("Invalid data")}


	ok = info.kdb.Contains("ABC");
	if ok {t.Errorf("data is not removed")}
}


func TestRemove(t *testing.T) {
	info := tearUpWithData(t)
	defer info.tearDown()
	ok := info.kdb.Remove("ABC")
	if !ok {t.Errorf("Cannot remove data")}

	ok = info.kdb.Contains("ABC");
	if ok {t.Errorf("data is not removed")}

	ok = info.kdb.Contains("1");
	if !ok {t.Errorf("Wrong data is removed")}
}

func TestKeyList(t *testing.T) {
	info := tearUpWithData(t)

	list, err := info.kdb.KeyList()
	if err != nil {t.Errorf("Some error %s", err.Error())}
	if len(list) != 4 {t.Errorf("Invalid number of keys")}

	expectedKeys := [...]string{"A", "z", "1", "ABC"}
	
	for _, v := range expectedKeys {
		found := false
		for _, r := range expectedKeys {
			if r == v {found = true}
		}
		if ! found {
			t.Errorf("Cannot find %s", v)
		}
	}

	info.tearDown()
}

func TestKeyIter(t *testing.T) {
	info := tearUpWithData(t)

	iter, err := info.kdb.KeyIter()
	if err != nil {t.Errorf("Some error %s", err.Error())}

	expectedKeys := map[string]bool{"A":false, "z":false, "1":false, "ABC":false}

	for {
		v, ok := <- iter
		if !ok {break}
		expectedKeys[v] = true
	}

	for k, v := range expectedKeys {
		if v == false {
			t.Errorf("%s is not found", k)
		}
	}

	info.tearDown()
}

func TestIter(t *testing.T) {
	info := tearUpWithData(t)

	iter, err := info.kdb.Iter()
	if err != nil {t.Errorf("Some error %s", err.Error())}

	expectedKeys := map[KyotoDBRecord]bool{
		KyotoDBRecord{"A", "B"}:false,
		KyotoDBRecord{"z", "y"}:false,
		KyotoDBRecord{"1", "2"}:false,
		KyotoDBRecord{"ABC", "124"}:false}

	for {
		v, ok := <- iter
		if !ok {break}
		expectedKeys[v] = true
	}

	for k, v := range expectedKeys {
		if v == false {
			t.Errorf("%s is not found", k)
		}
	}

	info.tearDown()
}

