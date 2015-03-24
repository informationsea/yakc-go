package yakc

// #cgo pkg-config: kyotocabinet
// #include <stdlib.h>
// #include <kclangc.h>
// #include <string.h>
import "C"

import ("fmt"; "unsafe")

type KyotoDBRecord struct {
	Key string
	Value string
}

type KyotoDB struct {
	vp *C.struct___1
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
		err = kyotoError(kdb)
		C.kcdbdel(p);
		return
	}

	err = nil
	return
}

func (kdb *KyotoDB) Close() error {
	if int(C.kcdbclose(kdb.vp)) == 0 {return kyotoError(kdb)}
	C.kcdbdel(kdb.vp)
	return nil
}

func (kdb *KyotoDB) Clear() error {
	if int(C.kcdbclear(kdb.vp)) == 0 {return kyotoError(kdb)}
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

func (kdb *KyotoDB) Pop(key string) (value string, err error) {
	err = nil
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	
	keyClen, _ := C.strlen(keyCstr)
	
	var valueLength C.size_t
	valueCstr, _ := C.kcdbseize(kdb.vp, keyCstr, keyClen, &valueLength)
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

func (kdb *KyotoDB) Append(key string, value string) (err error) {
	err = nil
	
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	keyClen, _ := C.strlen(keyCstr)

	valueCstr := C.CString(value)
	defer C.free(unsafe.Pointer(valueCstr))
	valueClen, _ := C.strlen(valueCstr)

	ret, _ := C.kcdbappend(kdb.vp, keyCstr, keyClen, valueCstr, valueClen)
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

func (kdb *KyotoDB) Remove(key string) bool {
	keyCstr := C.CString(key)
	defer C.free(unsafe.Pointer(keyCstr))
	keyClen, _ := C.strlen(keyCstr)

	ret, _ := C.kcdbremove(kdb.vp, keyCstr, keyClen)

	if int(ret) < 0 {return false}
	return true
}

func (kdb *KyotoDB) Count() (count int, err error) {
	Ccount, _ := C.kcdbcount(kdb.vp)

	if int(Ccount) < 0 {
		err = kyotoError(kdb)
		return
	}
	count = int(Ccount)
	return
}

func (kdb *KyotoDB) KeyList() (list []string, err error) {
	count, err := kdb.Count()
	if err != nil {return}
	list = make([]string, count)
	cur, _ := C.kcdbcursor(kdb.vp)
	if cur == nil {return}

	success := C.kccurjump(cur)
	if int(success) == 0 {
		err = kyotoError(kdb)
		return
	}

	for i := 0; i < count; i++ {
		var strClen C.size_t
		strCkey := C.kccurgetkey(cur, &strClen, 1)
		if strCkey == nil {
			err = kyotoError(kdb)
			return
		}
		list[i] = C.GoStringN(strCkey, C.int(strClen))
		C.kcfree(unsafe.Pointer(strCkey))
	}
	return
}

func (kdb *KyotoDB) KeyIter() (iter chan string, err error) {
	count, err := kdb.Count()
	if err != nil {return}
	cur, _ := C.kcdbcursor(kdb.vp)
	if cur == nil {return}

	success := C.kccurjump(cur)
	if int(success) == 0 {
		err = kyotoError(kdb)
		return
	}

	iter = make(chan string)

	go func() {
		for i := 0; i < count; i++ {
			var strClen C.size_t
			strCkey := C.kccurgetkey(cur, &strClen, 1)
			if strCkey == nil {
				err = kyotoError(kdb)
				return
			}
			iter <- C.GoStringN(strCkey, C.int(strClen))
			C.kcfree(unsafe.Pointer(strCkey))
		}
		close(iter)
	}()
	return
}

func (kdb *KyotoDB) Iter() (iter chan KyotoDBRecord, err error) {
	count, err := kdb.Count()
	if err != nil {return}
	cur, _ := C.kcdbcursor(kdb.vp)
	if cur == nil {return}

	success := C.kccurjump(cur)
	if int(success) == 0 {
		err = kyotoError(kdb)
		return
	}

	iter = make(chan KyotoDBRecord, 10)

	go func() {
		defer close(iter)
		for i := 0; i < count; i++ {
			var strCkeylen C.size_t
			strCkey := C.kccurgetkey(cur, &strCkeylen, 0)
			if strCkey == nil {
				err = kyotoError(kdb)
				fmt.Printf("Error %s\n", err)
				return
			}

			var strCvaluelen C.size_t
			strCvalue := C.kccurgetvalue(cur, &strCvaluelen, 1)
			if strCvalue == nil {
				err = kyotoError(kdb)
				fmt.Printf("Error %s\n", err)
				return
			}
			
			iter <- KyotoDBRecord{C.GoStringN(strCkey, C.int(strCkeylen)),
				C.GoStringN(strCvalue, C.int(strCvaluelen))}
			C.kcfree(unsafe.Pointer(strCkey))
			C.kcfree(unsafe.Pointer(strCvalue))
		}
	}()
	return
}


func kyotoError(kdb *KyotoDB) KyotoDBError {
	return KyotoDBError{int(C.kcdbecode(kdb.vp)),
		C.GoString(C.kcdbemsg(kdb.vp)),
		false}
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
