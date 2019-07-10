package address

type DataAddr interface {
	Get(sid, rid int32) string
}
