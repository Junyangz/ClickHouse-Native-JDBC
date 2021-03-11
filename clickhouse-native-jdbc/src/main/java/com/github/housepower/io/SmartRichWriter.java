/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.io;

import com.github.housepower.misc.Switcher;
import io.airlift.compress.Compressor;

import javax.annotation.Nullable;
import java.nio.charset.Charset;

public class SmartRichWriter implements RichWriter, SupportCompress {

    private final Switcher<RichWriter> switcher;
    private final boolean enableCompress;

    public SmartRichWriter(ByteBufRichWriter writer, boolean enableCompress, @Nullable Compressor compressor) {
        this.enableCompress = enableCompress;
        RichWriter compressWriter = null;
        if (enableCompress) {
            compressWriter = new CompressByteBufRichWriter(writer, compressor);
        }
        switcher = new Switcher<>(compressWriter, writer);
    }


    @Override
    public void writeBoolean(boolean b) {
        switcher.get().writeBoolean(b);
    }

    @Override
    public void writeByte(byte b) {
        switcher.get().writeByte(b);
    }

    @Override
    public void writeShortLE(short s) {
        switcher.get().writeShortLE(s);
    }

    @Override
    public void writeIntLE(int i) {
        switcher.get().writeIntLE(i);
    }

    @Override
    public void writeLongLE(long l) {
        switcher.get().writeLongLE(l);
    }

    @Override
    public void writeVarInt(long v) {
        switcher.get().writeVarInt(v);
    }

    @Override
    public void writeFloatLE(float f) {
        switcher.get().writeFloatLE(f);
    }

    @Override
    public void writeDoubleLE(double d) {
        switcher.get().writeDoubleLE(d);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        switcher.get().writeBytes(bytes);
    }

    @Override
    public void writeUTF8Binary(String utf8) {
        switcher.get().writeUTF8Binary(utf8);
    }

    @Override
    public void writeStringBinary(String seq, Charset charset) {
        switcher.get().writeStringBinary(seq, charset);
    }

    @Override
    public void writeBytesBinary(byte[] bytes) {
        switcher.get().writeBytesBinary(bytes);
    }

    @Override
    public void flush(boolean force) {
        switcher.get().flush(force);
    }

    @Override
    public void maybeEnableCompressed() {
        if (enableCompress)
            switcher.select(false);
    }

    @Override
    public void maybeDisableCompressed() {
        if (enableCompress)
            switcher.select(true);
    }
}
