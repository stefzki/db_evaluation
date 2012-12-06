package de.strud.data;

/**
 * Data object that represents a wikipedia abstract document.
 * User: strud
 */
public class Document {

    private String url;

    private String title;

    private String text;

    public Document(){

    }

    public Document(String url, String title, String text) {
        this.url = url;
        this.title = title;
        this.text = text;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "Document{" +
                "url='" + url + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
