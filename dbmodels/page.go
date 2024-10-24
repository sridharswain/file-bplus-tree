package dbmodels

type Page struct {
	DataOffset int64
	FileOffset uint8
}

type SortParamLocation struct {
	SortParam any
	Locations []*Page
}

type PageAndData struct {
	PageData *Page
	Data     []byte
}

func NewPage(offset int64) *Page {
	return &Page{DataOffset: offset}
}
