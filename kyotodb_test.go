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

