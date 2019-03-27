package top;

public class PageCount  implements Comparable<PageCount>{
    private String page;
    private int count;

    public PageCount(String page, int count) {
        this.page = page;
        this.count = count;
    }


    public String getPage() {
        return page;
    }
    public void setPage(String page, int count) {
        this.page = page;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        //如果浏览数相同比较，网页名称，不然就比较浏览数
        return o.getCount()-this.count==0?this.page.compareTo(o.getPage()):o.getCount()-this.count;
    }

}
