package com.ela;

import org.joda.time.LocalDate;

public class CodeTest {
    public static void main(String[] args) throws Exception {
        String content="刘沉二维码";
        String des="f://images/"+ LocalDate.now()+".png";
        QRCodeUtil.qrCode(content,des);
    }
}
