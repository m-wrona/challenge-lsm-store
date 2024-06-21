package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Model_CalculateTFIDF(t *testing.T) {
	type doc struct {
		term  Term
		id    DocumentID
		tf    Freq
		tfidf Freq
	}
	type term struct {
		term Term
		df   Freq
		idf  Freq
	}
	tests := []struct {
		name          string
		documents     []Document
		expDocValues  []doc
		expTermValues []term
	}{
		{
			name: "single document with many terms",
			documents: []Document{
				{
					DocId:         1,
					Term:          "Rammstein",
					TermFrequency: 45,
				},
				{
					DocId:         1,
					Term:          "in",
					TermFrequency: 11,
				},
				{
					DocId:         1,
					Term:          "Barcelona",
					TermFrequency: 33,
				},
			},
			expDocValues: []doc{
				{
					term:  "Rammstein",
					id:    1,
					tf:    45,
					tfidf: 0,
				},
			},
			expTermValues: []term{
				{
					term: "Rammstein",
					df:   1,
					idf:  0,
				},
			},
		},
		{
			name: "the same terms across documents",
			documents: []Document{
				{
					DocId:         1,
					Term:          "Rammstein",
					TermFrequency: 45,
				},
				{
					DocId:         2,
					Term:          "Rammstein",
					TermFrequency: 90,
				},
				{
					DocId:         1,
					Term:          "in",
					TermFrequency: 11,
				},
				{
					DocId:         2,
					Term:          "in",
					TermFrequency: 22,
				},
				{
					DocId:         1,
					Term:          "Barcelona",
					TermFrequency: 33,
				},
				{
					DocId:         2,
					Term:          "Barcelona",
					TermFrequency: 66,
				},
			},
			expDocValues: []doc{
				{
					term:  "Rammstein",
					id:    1,
					tf:    45,
					tfidf: 0,
				},
				{
					term:  "Rammstein",
					id:    2,
					tf:    90,
					tfidf: 0,
				},
			},
			expTermValues: []term{
				{
					term: "Rammstein",
					df:   2,
					idf:  0,
				},
			},
		},
		{
			name: "different terms across documents",
			documents: []Document{
				{
					DocId:         1,
					Term:          "Rammstein",
					TermFrequency: 1,
				},
				{
					DocId:         1,
					Term:          "is",
					TermFrequency: 1,
				},
				{
					DocId:         1,
					Term:          "the",
					TermFrequency: 1,
				},
				{
					DocId:         1,
					Term:          "best",
					TermFrequency: 1,
				},

				{
					DocId:         2,
					Term:          "Rammstein",
					TermFrequency: 2,
				},
				{
					DocId:         3,
					Term:          "Rammstein",
					TermFrequency: 3,
				},

				{
					DocId:         4,
					Term:          "Celine",
					TermFrequency: 4,
				},
				{
					DocId:         4,
					Term:          "Dion",
					TermFrequency: 4,
				},
			},
			expDocValues: []doc{
				{
					term:  "Rammstein",
					id:    1,
					tf:    1,
					tfidf: 0.124938734,
				},
				{
					term:  "Celine",
					id:    4,
					tf:    4,
					tfidf: 2.40824,
				},
			},
			expTermValues: []term{
				{
					term: "Rammstein",
					df:   3,
					idf:  0.124938734,
				},
				{
					term: "Celine",
					df:   1,
					idf:  0.60206,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := NewTFIDF()
			for _, d := range tt.documents {
				i.Add(d)
			}

			for _, expDoc := range tt.expDocValues {
				assert.Equalf(t, expDoc.tf, i.TF(expDoc.term, expDoc.id), "unexpected TF - term: %s, doc: %d",
					expDoc.term, expDoc.id)
				assert.Equalf(t, expDoc.tfidf, i.TFIDF(expDoc.term, expDoc.id), "unexpected TFIDF - term: %s, doc: %d",
					expDoc.term, expDoc.id)

			}

			for _, expTerm := range tt.expTermValues {
				assert.Equalf(t, expTerm.df, i.DF(expTerm.term), "unexpected DF - term: %s", expTerm.term)
				assert.Equalf(t, expTerm.idf, i.IDF(expTerm.term), "unexpected IDF - term: %s", expTerm.term)

			}
		})
	}
}
