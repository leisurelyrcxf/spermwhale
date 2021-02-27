package concurrency

import (
	"container/list"
	"sync"
)

type ConcurrentList struct {
	*list.List
	sync.Mutex
}

func NewConcurrentList() *ConcurrentList {
	return &ConcurrentList{
		List: list.New(),
	}
}

type ConcurrentListElement struct {
	CList *ConcurrentList
	*list.Element
}

func NewConcurrentListElement(cl *ConcurrentList, ele *list.Element) *ConcurrentListElement {
	return &ConcurrentListElement{
		CList:   cl,
		Element: ele,
	}
}
