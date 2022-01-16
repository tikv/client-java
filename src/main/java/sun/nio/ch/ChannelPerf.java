package sun.nio.ch;

import java.nio.channels.SocketChannel;

public class ChannelPerf {
  public static int getSocketChannelFD(SocketChannel sc) {
    return ((SocketChannelImpl) sc).getFDVal();
  }
}
