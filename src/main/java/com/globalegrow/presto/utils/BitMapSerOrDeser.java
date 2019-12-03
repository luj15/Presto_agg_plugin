package com.globalegrow.presto.utils;

import org.roaringbitmap.RoaringBitmap;

import java.io.*;

/**
 * @author 小和尚
 * @version 1.0.0
 * @ClassName ProstoPlugin.BitMapSerOrDeser
 * @Description TODO
 * @createTime 2019年11月29日 14:31:00
 */
public class BitMapSerOrDeser {
    /**
     * 将bitmap序列化成为字符串
     *
     * @param bitmap bitmap对象。
     * @return string 字符串
     */

    public static String serialize(RoaringBitmap bitmap) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(512);
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        try {
            bitmap.serialize(out);
            byte[] byteArray = byteArrayOutputStream.toByteArray();
            if (byteArray == null || byteArray.length < 1)
                throw new IllegalArgumentException("this byteArray must not be null or empty");

            final StringBuilder hexString = new StringBuilder();
            for (int i = 0; i < byteArray.length; i++) {
                if ((byteArray[i] & 0xff) < 0x10)
                    hexString.append("0");
                hexString.append(Integer.toHexString(0xFF & byteArray[i]));
            }
            return hexString.toString().toLowerCase();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize bitmap ", e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将String反序列化成为bitmap对象
     *
     * @param str bitmap序列化后的字符串。
     * @return bitmap RoaringBitmap对象
     */
    public static RoaringBitmap getBitMapObject(String str) throws IOException {

        RoaringBitmap bitmap = new RoaringBitmap();
        byte[] bytes = toByteArray(str);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteArrayInputStream);
        bitmap.deserialize(in);
        in.close();
        byteArrayInputStream.close();
        return bitmap;
    }

    /**
     * 将字符串转换成byte数组
     *
     * @param hexString 字符串。
     * @return byte数组
     */
    public static byte[] toByteArray(String hexString) {
        if (hexString.isEmpty())
            throw new IllegalArgumentException("this hexString must not be empty");

        hexString = hexString.toLowerCase();
        final byte[] byteArray = new byte[hexString.length() / 2];
        int k = 0;
        for (int i = 0; i < byteArray.length; i++) {//因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
            byte high = (byte) (Character.digit(hexString.charAt(k), 16) & 0xff);
            byte low = (byte) (Character.digit(hexString.charAt(k + 1), 16) & 0xff);
            byteArray[i] = (byte) (high << 4 | low);
            k += 2;
        }
        return byteArray;
    }

}