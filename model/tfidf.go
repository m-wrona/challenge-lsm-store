package model

import "math"

type (
	Freq = float32

	TFIDF struct {
		documents map[DocumentID]struct{}
		tf        map[Term]map[DocumentID]DocumentFreq
	}

	TFIDFValue struct {
		ID    DocumentID
		Term  string
		Value Freq
	}
)

func NewTFIDF() *TFIDF {
	return &TFIDF{
		documents: make(map[DocumentID]struct{}),
		tf:        make(map[Term]map[DocumentID]DocumentFreq),
	}
}

func (i *TFIDF) Add(d Document) {
	if d.TermFrequency == 0 {
		return
	}

	i.documents[d.DocId] = struct{}{}
	i.addTerm(d.Term, d.DocId, d.TermFrequency)
}

func (i *TFIDF) addTerm(t Term, id DocumentID, v DocumentFreq) {
	tf, ok := i.tf[t]
	if !ok {
		tf = make(map[DocumentID]DocumentFreq)
		i.tf[t] = tf
	}
	tf[id] = v
}

func (i *TFIDF) DF(t Term) Freq {
	return Freq(len(i.tf[t]))
}

func (i *TFIDF) TF(t Term, id DocumentID) Freq {
	tf := i.tf[t]
	return Freq(tf[id])
}

func (i *TFIDF) IDF(t Term) Freq {
	documents := float64(len(i.documents))
	df := float64(i.DF(t))
	return Freq(math.Log10(documents / df))
}

func (i *TFIDF) TFIDF(t Term, id DocumentID) Freq {
	return i.TF(t, id) * i.IDF(t)
}

func (i *TFIDF) Values() map[DocumentID][]TFIDFValue {
	docs := make(map[DocumentID][]TFIDFValue)

	for id := range i.documents {
		values := make([]TFIDFValue, 0)
		for term := range i.tf {
			idf := i.TFIDF(term, id)
			values = append(values, TFIDFValue{
				ID:    id,
				Term:  term,
				Value: idf,
			})
		}
		docs[id] = values
	}

	return docs
}
