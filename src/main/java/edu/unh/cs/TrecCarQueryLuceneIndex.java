package edu.unh.cs;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.CborFileTypeException;
import edu.unh.cs.treccar_v2.read_data.CborRuntimeException;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.*;

/*
 * User: dietz
 * Date: 1/4/18
 * Time: 1:23 PM
 */

/**
 * Example of how to build a lucene index of trec car paragraphs
 */
public class TrecCarQueryLuceneIndex {

    private static void usage() {
        System.out.println("Command line parameters: action OutlineCBOR LuceneINDEX (benchmarkY2.topics for action 'iterate.topics')\n" +
                "action is one of output-sections | paragraphs-run-sections | paragraphs-run-pages | pages-run-pages | iterate-topics");
        System.exit(-1);
    }

    static class MyQueryBuilder {

        private final StandardAnalyzer analyzer;
        private List<String> tokens;

        public MyQueryBuilder(StandardAnalyzer standardAnalyzer) {
            analyzer = standardAnalyzer;
            tokens = new ArrayList<>(128);
        }

        public BooleanQuery toQuery(String queryStr) throws IOException {

            TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(queryStr));
            tokenStream.reset();
            tokens.clear();
            while (tokenStream.incrementToken()) {
                final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                tokens.add(token);
            }
            tokenStream.end();
            tokenStream.close();
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
            for (String token : tokens) {
                booleanQuery.add(new TermQuery(new Term("text", token)), BooleanClause.Occur.SHOULD);
            }
            return booleanQuery.build();
        }
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("file.encoding", "UTF-8");

        if (args.length < 3)
            usage();


        String mode = args[0];
        String indexPath = args[2];

        if (mode.equals("output-sections")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                System.out.println("\n\nPage: " + page.getPageId());
                for (List<Data.Section> sectionPath : page.flatSectionPaths()) {
                    System.out.println();
                    System.out.println(Data.sectionPathId(page.getPageId(), sectionPath) + "   \t " + Data.sectionPathHeadings(sectionPath));

                    String queryStr = buildSectionQueryStr(page, sectionPath);

                    // get top 10 documents
                    TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 10);
                    ScoreDoc[] scoreDoc = tops.scoreDocs;
                    System.out.println("Found " + scoreDoc.length + " results.");
                    for (ScoreDoc score : scoreDoc) {
                        final Document doc = searcher.doc(score.doc); // to access stored content
                        // print score and internal docid
                        System.out.println(doc.getField("paragraphid").stringValue() + " (" + score.doc + "):  SCORE " + score.score + "\n");
                        // access and print content
//                        System.out.println("  " +doc.getField("text").stringValue());
                    }

                }
                System.out.println();
            }
        } else if (mode.equals("paragraphs-run-sections")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                for (List<Data.Section> sectionPath : page.flatSectionPaths()) {
                    final String queryId = Data.sectionPathId(page.getPageId(), sectionPath);

                    String queryStr = buildSectionQueryStr(page, sectionPath);

                    TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                    ScoreDoc[] scoreDoc = tops.scoreDocs;
                    for (int i = 0; i < scoreDoc.length; i++) {
                        ScoreDoc score = scoreDoc[i];
                        final Document doc = searcher.doc(score.doc); // to access stored content
                        // print score and internal docid
                        final String paragraphid = doc.getField("paragraphid").stringValue();
                        final float searchScore = score.score;
                        final int searchRank = i + 1;

                        System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                    }

                }
            }
        } else if (mode.equals("paragraphs-run-pages")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                final String queryId = page.getPageId();

                String queryStr = buildSectionQueryStr(page, Collections.<Data.Section>emptyList());

                TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                ScoreDoc[] scoreDoc = tops.scoreDocs;
                for (int i = 0; i < scoreDoc.length; i++) {
                    ScoreDoc score = scoreDoc[i];
                    final Document doc = searcher.doc(score.doc); // to access stored content
                    // print score and internal docid
                    final String paragraphid = doc.getField("paragraphid").stringValue();
                    final float searchScore = score.score;
                    final int searchRank = i + 1;

                    System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                }

            }
        } else if (mode.equals("pages-run-pages")) {
            IndexSearcher searcher = setupIndexSearcher(indexPath, "pages.lucene");

            searcher.setSimilarity(new BM25Similarity());
            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());

            final String pagesFile = args[1];
            final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
            for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
                final String queryId = page.getPageId();

                String queryStr = buildSectionQueryStr(page, Collections.<Data.Section>emptyList());

                TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
                ScoreDoc[] scoreDoc = tops.scoreDocs;
                for (int i = 0; i < scoreDoc.length; i++) {
                    ScoreDoc score = scoreDoc[i];
                    final Document doc = searcher.doc(score.doc); // to access stored content
                    // print score and internal docid
                    final String paragraphid = doc.getField("pageid").stringValue();
                    final float searchScore = score.score;
                    final int searchRank = i + 1;

                    System.out.println(queryId + " Q0 " + paragraphid + " " + searchRank + " " + searchScore + " Lucene-BM25");
                }

            }

        } else if (mode.equals("iterate-topics")) {

            IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene"); //Create IndexSearcher

            searcher.setSimilarity(new BM25Similarity());

            final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());

            String titlesFilePath = args[1];  //Get benchmarkY2.titles file

            try {
                List<String> lines = Files.readAllLines(Paths.get(args[3]), StandardCharsets.UTF_8);

                BufferedReader br = new BufferedReader(new FileReader(titlesFilePath));
                String line = br.readLine();
                File resultFile = new File("trec_eval_input.txt");
                resultFile.delete();
                resultFile.createNewFile();
                while (line != null) {  //Iterate over every line

                    TopDocs tops = searcher.search(queryBuilder.toQuery(line), 100); //Finds 100 docs for every query

                    ScoreDoc[] scoreDoc = tops.scoreDocs;   //Scores the retrieved docs
                    final String[] splittedLine = line.split(" ");

                    String queryID = lines.stream()
                            .filter(str -> Arrays.stream(splittedLine).allMatch(str::contains))
                            .findFirst()
                            .orElse("N/A")
                            .split("/")[0];

                    FileWriter fw = new FileWriter(resultFile, true);
                    BufferedWriter bw = new BufferedWriter(fw);
                    PrintWriter out = new PrintWriter(bw);

                    //Iterate over the scored docs
                    for (int i = 0; i < scoreDoc.length; i++) {

                        ScoreDoc score = scoreDoc[i];

                        final Document doc = searcher.doc(score.doc); // to access stored content

                        final String paragraphID = doc.getField("paragraphid").stringValue();

                        final float searchScore = score.score;

                        final int searchRank = i + 1;

                        out.println(queryID + " Q0 " + paragraphID + " " + searchRank + " " + searchScore + " Lucene-BM25");
                        System.out.println("In here");
                    }

                    if (line.equals(""))
                        break;

                    line = br.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @NotNull
    private static IndexSearcher setupIndexSearcher(String indexPath, String typeIndex) throws IOException {
        Path path = FileSystems.getDefault().getPath(indexPath, typeIndex);
        Directory indexDir = FSDirectory.open(path);
        IndexReader reader = DirectoryReader.open(indexDir);
        return new IndexSearcher(reader);
    }

    @NotNull
    private static String buildSectionQueryStr(Data.Page page, List<Data.Section> sectionPath) {
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(page.getPageName());
        for (Data.Section section : sectionPath) {
            queryStr.append(" ").append(section.getHeading());
        }
//        System.out.println("queryStr = " + queryStr);
        return queryStr.toString();
    }

    private static Iterable<Document> toIterable(final Iterator<Document> iter) throws CborRuntimeException, CborFileTypeException {
        return new Iterable<Document>() {
            @Override
            @NotNull
            public Iterator<Document> iterator() {
                return iter;
            }
        };
    }

}