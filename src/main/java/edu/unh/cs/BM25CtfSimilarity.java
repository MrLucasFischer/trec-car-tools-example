/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unh.cs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

/**
 * BM25 Similarity. Introduced in Stephen E. Robertson, Steve Walker,
 * Susan Jones, Micheline Hancock-Beaulieu, and Mike Gatford. Okapi at TREC-3.
 * In Proceedings of the Third <b>T</b>ext <b>RE</b>trieval <b>C</b>onference (TREC 1994).
 * Gaithersburg, USA, November 1994.
 */
public class BM25CtfSimilarity extends Similarity {
    private final float k1;
    private final float b;
    private final float d;

    /**
     * BM25 with the supplied parameter values.
     *
     * @param k1 Controls non-linear term frequency normalization (saturation).
     * @param b  Controls to what degree document length normalizes tf values.
     * @param d  Controls lower-bound of term frequency normalization in BM25.
     * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is
     *                                  not within the range {@code [0..1]}
     */
    public BM25CtfSimilarity(float k1, float b, float d) {
        if (Float.isFinite(k1) == false || k1 < 0) {
            throw new IllegalArgumentException("illegal k1 value: " + k1 + ", must be a non-negative finite value");
        }
        if (Float.isNaN(b) || b < 0 || b > 1) {
            throw new IllegalArgumentException("illegal b value: " + b + ", must be between 0 and 1");
        }
        if (Float.isNaN(d) || d < 0 || d > 1) {
            throw new IllegalArgumentException("illegal d value: " + d + ", must be between 0 and 1");
        }
        this.k1 = k1;
        this.b = b;
        this.d = d;
    }

    /**
     * BM25 with these default values:
     * <ul>
     * <li>{@code k1 = 1.2}</li>
     * <li>{@code b = 0.75}</li>
     * li>{@code d = 0.5}</li>
     * </ul>
     */
    public BM25CtfSimilarity() {
        this(1.2f, 0.75f, 0.5f);
    }

    /**
     * Implemented as <code>log(sumTotalTermFreq/totalTermFreq</code>.
     */
    protected float ictf(long totalTermFreq, long sumTotalTermFreq) {
        return (float) Math.log((float) sumTotalTermFreq / totalTermFreq);
    }

    /**
     * Implemented as code>log(1 + p_df/p_ctf)</code>.
     */
    protected float pidf(long docFreq, long docCount, long totalTermFreq) {
        float pdf = docCount * (1 - 1 / (float) Math.exp((float) totalTermFreq / docCount));
        float pctf = -docCount * (float) Math.log(1 - (float) docFreq / docCount);
        return (float) Math.log(1 + pdf / pctf);
    }

    /**
     * Implemented as <code>log((docCount+1)/(docFreq+1)) + 1</code>.
     */
    protected float idf(long docFreq, long docCount) {
        return (float) (Math.log((docCount + 1) / (double) (docFreq + 1)) + 1.0);
    }

    /**
     * Implemented as <code>1 / (distance + 1)</code>.
     */
    protected float sloppyFreq(int distance) {
        return 1.0f / (distance + 1);
    }

    /**
     * The default implementation returns <code>1</code>
     */
    protected float scorePayload(int doc, int start, int end, BytesRef payload) {
        return 1;
    }

    /**
     * The default implementation computes the average as <code>sumTotalTermFreq / docCount</code>
     */
    protected float avgFieldLength(CollectionStatistics collectionStats) {
        final long sumTotalTermFreq;
        if (collectionStats.sumTotalTermFreq() == -1) {
            // frequencies are omitted (tf=1), its # of postings
            if (collectionStats.sumDocFreq() == -1) {
                // theoretical case only: remove!
                return 1f;
            }
            sumTotalTermFreq = collectionStats.sumDocFreq();
        } else {
            sumTotalTermFreq = collectionStats.sumTotalTermFreq();
        }
        final long docCount = collectionStats.docCount() == -1 ? collectionStats.maxDoc() : collectionStats.docCount();
        return (float) (sumTotalTermFreq / (double) docCount);
    }

    /**
     * True if overlap tokens (tokens with a position of increment of zero) are
     * discounted from the document's length.
     */
    protected boolean discountOverlaps = true;

    /**
     * Sets whether overlap tokens (Tokens with 0 position increment) are
     * ignored when computing norm.  By default this is true, meaning overlap
     * tokens do not count when computing norms.
     */
    public void setDiscountOverlaps(boolean v) {
        discountOverlaps = v;
    }

    /**
     * Returns true if overlap tokens are discounted from the document's length.
     *
     * @see #setDiscountOverlaps
     */
    public boolean getDiscountOverlaps() {
        return discountOverlaps;
    }

    /**
     * Cache of decoded bytes.
     */
    private static final float[] OLD_LENGTH_TABLE = new float[256];
    private static final float[] LENGTH_TABLE = new float[256];

    static {
        for (int i = 1; i < 256; i++) {
            float f = SmallFloat.byte315ToFloat((byte) i);
            OLD_LENGTH_TABLE[i] = 1.0f / (f * f);
        }
        OLD_LENGTH_TABLE[0] = 1.0f / OLD_LENGTH_TABLE[255]; // otherwise inf

        for (int i = 0; i < 256; i++) {
            LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
        }
    }


    @Override
    public final long computeNorm(FieldInvertState state) {
        final int numTerms = discountOverlaps ? state.getLength() - state.getNumOverlap() : state.getLength();
        int indexCreatedVersionMajor = state.getIndexCreatedVersionMajor();
        if (indexCreatedVersionMajor >= 7) {
            return SmallFloat.intToByte4(numTerms);
        } else {
            return SmallFloat.floatToByte315((float) (1 / Math.sqrt(numTerms)));
        }
    }

    /**
     * Computes a score factor for a simple term and returns an explanation
     * for that score factor.
     *
     * <p>
     * The default implementation uses:
     *
     * <pre class="prettyprint">
     * idf(docFreq, docCount);
     * </pre>
     * <p>
     * Note that {@link CollectionStatistics#docCount()} is used instead of
     * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also
     * {@link TermStatistics#docFreq()} is used, and when the latter
     * is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
     * In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the term
     * @return an Explain object that includes both an idf score factor
     * and an explanation for the term.
     */
    public Explanation ictfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
        final long ctf = termStats.totalTermFreq();
        final long sumTotalTermFreq = collectionStats.sumTotalTermFreq();
        final float ictf = ictf(ctf, sumTotalTermFreq);
        return Explanation.match(ictf, "ictf, computed as log(M / ctf) from:",
                Explanation.match(sumTotalTermFreq, "M, total number of tokens in the field"),
                Explanation.match(ctf, "ctf, total number of occurrences of this term"));
    }

    /**
     * Computes a score factor for a phrase.
     *
     * <p>
     * The default implementation sums the idf factor for
     * each term in the phrase.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the terms in the phrase
     * @return an Explain object that includes both an idf
     * score factor for the phrase and an explanation
     * for each term.
     */
    public Explanation ictfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
        double ictf = 0d; // sum into a double before casting into a float
        List<Explanation> details = new ArrayList<>();
        for (final TermStatistics stat : termStats) {
            Explanation ictfExplain = ictfExplain(collectionStats, stat);
            details.add(ictfExplain);
            ictf += ictfExplain.getValue();
        }
        return Explanation.match((float) ictf, "ictf, sum of:", details);
    }

    /**
     * Computes a score factor for a simple term and returns an explanation
     * for that score factor.
     *
     * <p>
     * The default implementation uses:
     *
     * <pre class="prettyprint">
     * ictf(docFreq, docCount);
     * </pre>
     * <p>
     * Note that {@link CollectionStatistics#docCount()} is used instead of
     * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also
     * {@link TermStatistics#docFreq()} is used, and when the latter
     * is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
     * In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the term
     * @return an Explain object that includes both an idf score factor
     * and an explanation for the term.
     */
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
        final long df = termStats.docFreq();
        final long docCount = collectionStats.docCount() == -1 ? collectionStats.maxDoc() : collectionStats.docCount();
        final float idf = idf(df, docCount);
        return Explanation.match(idf, "idf, computed as log((docCount+1)/(docFreq+1)) + 1 from:",
                Explanation.match(df, "n, number of documents containing term"),
                Explanation.match(docCount, "N, total number of documents with field"));
    }

    /**
     * Computes a score factor for a phrase.
     *
     * <p>
     * The default implementation sums the idf factor for
     * each term in the phrase.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the terms in the phrase
     * @return an Explain object that includes both an idf
     * score factor for the phrase and an explanation
     * for each term.
     */
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
        double idf = 0d; // sum into a double before casting into a float
        List<Explanation> details = new ArrayList<>();
        for (final TermStatistics stat : termStats) {
            Explanation idfExplain = idfExplain(collectionStats, stat);
            details.add(idfExplain);
            idf += idfExplain.getValue();
        }
        return Explanation.match((float) idf, "idf, sum of:", details);
    }

    /**
     * Computes a score factor for a simple term and returns an explanation
     * for that score factor.
     *
     * <p>
     * The default implementation uses:
     *
     * <pre class="prettyprint">
     * pidf(docFreq, docCount);
     * </pre>
     * <p>
     * Note that {@link CollectionStatistics#docCount()} is used instead of
     * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also
     * {@link TermStatistics#docFreq()} is used, and when the latter
     * is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
     * In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the term
     * @return an Explain object that includes both an idf score factor
     * and an explanation for the term.
     */
    public Explanation pidfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
        final long df = termStats.docFreq();
        final long docCount = collectionStats.docCount();
        final long ctf = termStats.totalTermFreq();
        final float pidf = pidf(df, docCount, ctf);
        return Explanation.match(pidf, "pidf, computed as log(1 + p_df/p_ctf) from:",
                Explanation.match(df, "n, number of documents containing term"),
                Explanation.match(ctf, "ctf, total number of occurrences of this term"),
                Explanation.match(docCount, "N, total number of documents with field"));
    }

    /**
     * Computes a score factor for a phrase.
     *
     * <p>
     * The default implementation sums the idf factor for
     * each term in the phrase.
     *
     * @param collectionStats collection-level statistics
     * @param termStats       term-level statistics for the terms in the phrase
     * @return an Explain object that includes both an idf
     * score factor for the phrase and an explanation
     * for each term.
     */
    public Explanation pidfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
        double pidf = 0d; // sum into a double before casting into a float
        List<Explanation> details = new ArrayList<>();
        for (final TermStatistics stat : termStats) {
            Explanation pidfExplain = pidfExplain(collectionStats, stat);
            details.add(pidfExplain);
            pidf += pidfExplain.getValue();
        }
        return Explanation.match((float) pidf, "pidf, sum of:", details);
    }

    @Override
    public final SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        Explanation ictf = termStats.length == 1 ? ictfExplain(collectionStats, termStats[0]) : ictfExplain(collectionStats, termStats);
        Explanation pidf = termStats.length == 1 ? pidfExplain(collectionStats, termStats[0]) : pidfExplain(collectionStats, termStats);
        Explanation idf = termStats.length == 1 ? idfExplain(collectionStats, termStats[0]) : idfExplain(collectionStats, termStats);
        float avgdl = avgFieldLength(collectionStats);

        float[] oldCache = new float[256];
        float[] cache = new float[256];
        for (int i = 0; i < cache.length; i++) {
            oldCache[i] = k1 * ((1 - b) + b * OLD_LENGTH_TABLE[i] / avgdl);
            cache[i] = k1 * ((1 - b) + b * LENGTH_TABLE[i] / avgdl);
        }
        return new BM25Stats(collectionStats.field(), boost, ictf, pidf, idf, avgdl, oldCache, cache);
    }

    @Override
    public final SimScorer simScorer(SimWeight stats, LeafReaderContext context) throws IOException {
        BM25Stats bm25stats = (BM25Stats) stats;
        return new BM25DocScorer(bm25stats, context.reader().getMetaData().getCreatedVersionMajor(), context.reader().getNormValues(bm25stats.field));
    }

    private class BM25DocScorer extends SimScorer {
        private final BM25Stats stats;
        private final float weightValue; // boost * idf * (k1 + 1)
        private final NumericDocValues norms;
        /**
         * precomputed cache for all length values
         */
        private final float[] lengthCache;
        /**
         * precomputed norm[256] with k1 * ((1 - b) + b * dl / avgdl)
         */
        private final float[] cache;

        BM25DocScorer(BM25Stats stats, int indexCreatedVersionMajor, NumericDocValues norms) throws IOException {
            this.stats = stats;
            this.weightValue = stats.weight * (k1 + 1);
            this.norms = norms;
            if (indexCreatedVersionMajor >= 7) {
                lengthCache = LENGTH_TABLE;
                cache = stats.cache;
            } else {
                lengthCache = OLD_LENGTH_TABLE;
                cache = stats.oldCache;
            }
        }

        @Override
        public float score(int doc, float freq) throws IOException {
            // if there are no norms, we act as if b=0
            float norm;
            if (norms == null) {
                norm = k1;
            } else {
                if (norms.advanceExact(doc)) {
                    norm = cache[((byte) norms.longValue()) & 0xFF];
                } else {
                    norm = cache[0];
                }
            }
            return weightValue * freq / (freq + norm);
        }

        @Override
        public Explanation explain(int doc, Explanation freq) throws IOException {
            return explainScore(doc, freq, stats, norms, lengthCache);
        }

        @Override
        public float computeSlopFactor(int distance) {
            return sloppyFreq(distance);
        }

        @Override
        public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
            return scorePayload(doc, start, end, payload);
        }
    }

    /**
     * Collection statistics for the BM25 model.
     */
    private static class BM25Stats extends SimWeight {
        /**
         * BM25's ictf
         */
        private final Explanation ictf;
        /**
         * BM25's pidf
         */
        private final Explanation pidf;
        /**
         * BM25's idf
         */
        private final Explanation idf;
        /**
         * The average document length.
         */
        private final float avgdl;
        /**
         * query boost
         */
        private final float boost;
        /**
         * weight (idf * boost)
         */
        private final float weight;
        /**
         * field name, for pulling norms
         */
        private final String field;
        /**
         * precomputed norm[256] with k1 * ((1 - b) + b * dl / avgdl)
         * for both OLD_LENGTH_TABLE and LENGTH_TABLE
         */
        private final float[] oldCache, cache;

        BM25Stats(String field, float boost, Explanation ictf, Explanation pidf, Explanation idf, float avgdl, float[] oldCache, float[] cache) {
            this.field = field;
            this.boost = boost;
            this.ictf = ictf;
            this.pidf = pidf;
            this.idf = idf;
            this.avgdl = avgdl;
            this.weight = pidf.getValue() * idf.getValue() * boost;
            this.oldCache = oldCache;
            this.cache = cache;
        }

    }

    private Explanation explainTFNorm(int doc, Explanation freq, BM25Stats stats, NumericDocValues norms, float[] lengthCache) throws IOException {
        List<Explanation> subs = new ArrayList<>();
        subs.add(freq);
        subs.add(Explanation.match(k1, "parameter k1"));
        if (norms == null) {
            subs.add(Explanation.match(0, "parameter b (norms omitted for field)"));
            return Explanation.match(
                    (freq.getValue() * (k1 + 1)) / (freq.getValue() + k1),
                    "tfNorm, computed as (freq * (k1 + 1)) / (freq + k1) from:", subs);
        } else {
            byte norm;
            if (norms.advanceExact(doc)) {
                norm = (byte) norms.longValue();
            } else {
                norm = 0;
            }
            float doclen = lengthCache[norm & 0xff];
            subs.add(Explanation.match(b, "parameter b"));
            subs.add(Explanation.match(stats.avgdl, "avgFieldLength"));
            subs.add(Explanation.match(doclen, "fieldLength"));
            return Explanation.match(
                    (freq.getValue() * (k1 + 1)) / (freq.getValue() + k1 * (1 - b + b * doclen / stats.avgdl)),
                    "tfNorm, computed as (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * fieldLength / avgFieldLength)) from:", subs);
        }
    }

    private Explanation explainScore(int doc, Explanation freq, BM25Stats stats, NumericDocValues norms, float[] lengthCache) throws IOException {
        Explanation boostExpl = Explanation.match(stats.boost, "boost");
        List<Explanation> subs = new ArrayList<>();
        if (boostExpl.getValue() != 1.0f)
            subs.add(boostExpl);
        // ictf
//        subs.add(stats.ictf);
        // pidf
        subs.add(stats.pidf);
        // idf
        subs.add(stats.idf);
        Explanation tfNormExpl = explainTFNorm(doc, freq, stats, norms, lengthCache);
        subs.add(tfNormExpl);
        return Explanation.match(
                boostExpl.getValue() * stats.pidf.getValue() * stats.idf.getValue() * tfNormExpl.getValue(),
                "score(doc=" + doc + ",freq=" + freq + "), product of:", subs);
    }

    @Override
    public String toString() {
        return "BM25(k1=" + k1 + ",b=" + b + ")";
    }

    /**
     * Returns the <code>k1</code> parameter
     *
     * @see #BM25CtfSimilarity(float, float, float)
     */
    public final float getK1() {
        return k1;
    }

    /**
     * Returns the <code>b</code> parameter
     *
     * @see #BM25CtfSimilarity(float, float, float)
     */
    public final float getB() {
        return b;
    }

    /**
     * Returns the <code>d</code> parameter
     *
     * @see #BM25CtfSimilarity(float, float, float)
     */
    public final float getD() {
        return d;
    }
}
