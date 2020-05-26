package batching;

import java.io.Serializable;

public class WordCountRow implements Serializable {
    private String word;
    private Long count;

    public WordCountRow(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public Long getCount() {
        return count;
    }
}
