package types

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

type RetryDetailItem struct {
	errors.ErrorKey
	Count int
}

func (i RetryDetailItem) String() string {
	return fmt.Sprintf("\"%s\": %d", errors.AllErrors[i.ErrorKey].Msg, i.Count)
}

type RetryDetailItems []RetryDetailItem

func (r RetryDetailItems) Len() int           { return len(r) }
func (r RetryDetailItems) Less(i, j int) bool { return r[i].Count > r[j].Count }
func (r RetryDetailItems) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type RetryDetails map[errors.ErrorKey]int

func (d RetryDetails) Collect(another RetryDetails) {
	for errType, count := range another {
		d[errType] += count
	}
}

func (d RetryDetails) GetSortedRetryDetails() (items RetryDetailItems) {
	for errKey, count := range d {
		items = append(items, RetryDetailItem{
			ErrorKey: errKey,
			Count:    count,
		})
	}
	sort.Sort(items)
	return items
}

func (d RetryDetails) String() string {
	items := d.GetSortedRetryDetails()
	itemDesc := make([]string, len(items))
	for idx, item := range items {
		itemDesc[idx] = item.String()
	}
	return strings.Join(itemDesc, ", ")
}
