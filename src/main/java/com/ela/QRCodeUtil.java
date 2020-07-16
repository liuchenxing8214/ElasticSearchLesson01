package com.ela;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Hashtable;

public class QRCodeUtil {
    private static final String CHARSET = "utf-8";
    private static final String FORMAT_NAME = "PNG";
    // 二維碼尺寸
    private static final int QRCODE_SIZE = 510;

    /**
     * @param content 生成圖片內容或者跳轉地址
     * @param imgPath 合成圖片的路徑
     * @param des     生成圖片存儲的路徑
     * @throws Exception
     */
    public static void createImage(String content, String imgPath, String des) throws Exception {
        Hashtable hints = new Hashtable();
        hints.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H);
        hints.put(EncodeHintType.CHARACTER_SET, CHARSET);
        hints.put(EncodeHintType.MARGIN, 1);
        BitMatrix bitMatrix = new MultiFormatWriter().encode(content, BarcodeFormat.QR_CODE, QRCODE_SIZE, QRCODE_SIZE,
                hints);
        int width = bitMatrix.getWidth();
        int height = bitMatrix.getHeight();
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                image.setRGB(x, y, bitMatrix.get(x, y) ? 0xFF000000 : 0xFFFFFFFF);
            }
        }
        // 合成圖片
        QRCodeUtil.insertImage(image, imgPath, des);
    }

    /**
     * 合成圖片
     *
     * @param image    生成的二維碼圖片
     * @param imgPath  背景圖片的地址
     * @param destPath //合成新圖片的存儲地址
     * @throws Exception
     */
    private static void insertImage(BufferedImage image, String imgPath, String destPath) throws Exception {
        File file = new File(imgPath);
        if (!file.exists()) {
            System.err.println("" + imgPath + "   該文件不存在！");
            return;
        }
        BufferedImage src = ImageIO.read(new File(imgPath));
        int width = src.getWidth(null);
        int height = src.getHeight(null);
        Graphics2D graph = src.createGraphics();
        int x = (width - QRCODE_SIZE) / 2 + 20;
        int y = (height - QRCODE_SIZE) / 2 - 80;
        graph.drawImage(image, x, y, image.getWidth(null), image.getHeight(null), null);
        graph.dispose();
        //創建文件路徑
        mkdirs(destPath);
        /**
         * src 輸出的圖片
         * FORMAT_NAME 輸出圖片類型
         * destPath 輸出的文件地址
         */
        ImageIO.write(src, FORMAT_NAME, new File(destPath));
    }


    public static void mkdirs(String destPath) {
        File file = new File(destPath);
        // 當文件夾不存在時，mkdirs會自動創建多層目錄，區別於mkdir．(mkdir如果父目錄不存在則會拋出異常)
        if (!file.exists() && !file.isDirectory()) {
            file.mkdirs();
        }
    }


    /**
     * 直接生成並輸出二維碼圖片
     *
     * @param content  輸出二維碼的路徑或者二維碼跳轉的路徑
     * @param destPath 輸出的二維碼到本地磁盤的存儲路徑
     * @throws Exception
     */
    public static void qrCode(String content, String destPath) throws Exception {
        Hashtable hints = new Hashtable();
        hints.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H);
        hints.put(EncodeHintType.CHARACTER_SET, CHARSET);
        hints.put(EncodeHintType.MARGIN, 1);
        BitMatrix bitMatrix = new MultiFormatWriter().encode(content, BarcodeFormat.QR_CODE, QRCODE_SIZE, QRCODE_SIZE,
                hints);
        int width = bitMatrix.getWidth();
        int height = bitMatrix.getHeight();
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                image.setRGB(x, y, bitMatrix.get(x, y) ? 0xFF000000 : 0xFFFFFFFF);
            }
        }
        File file = new File(destPath);
        // 當文件夾不存在時，mkdirs會自動創建多層目錄，區別於mkdir．(mkdir如果父目錄不存在則會拋出異常)
        if (!file.exists() && !file.isDirectory()) {
            file.mkdirs();
        }
        // 插入圖片
        ImageIO.write(image, FORMAT_NAME, new File(destPath));
    }
}
