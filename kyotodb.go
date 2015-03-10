package yakc

// #cgo pkg-config: kyotocabinet
// #include <stdlib.h>
// #include <kclangc.h>
// #include <string.h>
import "C"

import ("fmt"; "unsafe")

type KyotoDB struct {
	vp *C.struct___0
}

type KyotoDBError struct {
	ErrorCode int
	ErrorMessage string
	NotFound bool
}

func Open(path string) (kdb *KyotoDB, err error) {
	p, _ := C.kcdbnew()
	if p == nil {err = fmt.Errorf("Cannot allocate memory"); return}

	pathCstr := C.CString(path)
	defer C.free(unsafe.Pointer(pathCstr))
	r, err := C.kcdbopen(p, pathCstr, C.KCOWRITER|C.KCOCREATE)
	kdb = &KyotoDB{p}

	if int(r) == 0 {
		err = fmt.Errorf("Failed to open DB (%s, %s)",
			errorType(kdb))
		C.kcdbdel(p);
		return
	}

	err = nil
	return
}

func (kdb *KyotoDB) Close() error {
	if int(C.kcdbclose(kdb.vp)) == 0 {return fmt.Errorf("Cannot close %s", errorType(kdb))}
	C.kcdbdel(kdb.vp)
	return nil
}

func (kdb *KyotoDB) Get(key string) (value string, err error) {
	err = nil
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	
	keyClen, _ := C.strlen(keyCstr)
	
	var valueLength C.size_t
	valueCstr, _ := C.kcdbget(kdb.vp, keyCstr, keyClen, &valueLength)
	defer C.kcfree(unsafe.Pointer(valueCstr))
	if valueCstr == nil {
		value = ""
		err = KyotoDBError{C.KCESUCCESS, "Not Found", true}
		return
	}
	
	value = C.GoStringN(valueCstr, C.int(valueLength))
	return
}

func (kdb *KyotoDB) GetOrDefault(key string, defaultValue string) (value string, err error) {
	value, err = kdb.Get(key)
	if err == nil {return}

	if kyotoError, ok := err.(KyotoDBError); ok {
		if kyotoError.NotFound {
			value = defaultValue
			err = nil
		}
	}
	
	return
}

func (kdb *KyotoDB) Set(key string, value string) (err error) {
	err = nil
	
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	keyClen, _ := C.strlen(keyCstr)

	valueCstr := C.CString(value)
	defer C.free(unsafe.Pointer(valueCstr))
	valueClen, _ := C.strlen(valueCstr)

	ret, _ := C.kcdbset(kdb.vp, keyCstr, keyClen, valueCstr, valueClen)
	if int(ret) == 0 {
		err = fmt.Errorf("Cannot set value %s = %s (%s)", key, value, errorType(kdb))
	}

	return
}

func (kdb *KyotoDB) Contains(key string) bool {
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	keyClen, _ := C.strlen(keyCstr)

	ret, _ := C.kcdbcheck(kdb.vp, keyCstr, keyClen)

	if int(ret) < 0 {return false}
	return true
}


func errorType(kdb *KyotoDB) string {
	return errorTypeString(int(C.kcdbecode(kdb.vp)))
}

func errorTypeString(code int) string {
	switch (code) {
	case C.KCESUCCESS:
		return "Success"
	case C.KCENOIMPL:
		return "not implemented"
	case C.KCEINVALID:
		return "invalid operation"
	case C.KCENOREPOS:
		return "no repository"
	default:
		return "Unknown"
	}
}

func (err KyotoDBError) Error() string {
	return err.ErrorMessage
}
