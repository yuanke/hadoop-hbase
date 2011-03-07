package org.apache.hadoop.hdfs.archivetool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.UTF8;

/*
 * @author Ruan Weichao
 * This class is for downloading and checking fsimage's difference of multi-hdfs
 */
@SuppressWarnings("deprecation")
public class ArchiveChecker {

  static private final UTF8 U_STR = new UTF8();
       static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");

       //Constructor
       public ArchiveChecker() {
               //do nothing
       }

       //Read string from DataInputStream via UTF8
  static String readString(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    return U_STR.toString();
  }

       //Print a single line
       private void printFileInfo(String path, boolean isDir, PermissionStatus ps,
           short replication, long size, long modificationTime) {
               int maxReplication = 3, maxLen = 10, maxOwner = 10, maxGroup = 10;

               String mdate = dateForm.format(new Date(modificationTime));
               System.out.print((isDir ? "d" : "-") + ps.getPermission() + " ");
               System.out.printf("%" + maxReplication + "s ", (!isDir ? replication : "-"));
               if (maxOwner > 0)
                       System.out.printf("%-" + maxOwner + "s ", ps.getUserName());
               if (maxGroup > 0)
                       System.out.printf("%-" + maxGroup + "s ", ps.getGroupName());
               System.out.printf("%" + maxLen + "d ", size);
               System.out.print(mdate + " ");
               System.out.println(path);
       }

       //For listing the whole file system via fsimage
  private void listFile(File curFile) throws IOException {
    assert curFile != null : "curFile is null";

    DataInputStream in = new DataInputStream(new BufferedInputStream(
                              new FileInputStream(curFile)));
    try {
      // read image version: first appeared in version -1
      int imgVersion = in.readInt();
      // read namespaceID: first appeared in version -2
      int namespaceID = in.readInt();

      // read number of files
      long numFiles;
      if (imgVersion <= -16) {
        numFiles = in.readLong();
      } else {
        numFiles = in.readInt();
      }

      // read in the last generation stamp.
      if (imgVersion <= -12) {
        long genstamp = in.readLong();
      }
      
      // read file info      
      short replication;
      String path;
      Block block = new Block();
      
      for (long i = 0; i < numFiles; i++) {
        long modificationTime = 0;
        long atime = 0;
        long blockSize = 0;
        path = readString(in);
        if (path.length() == 0)
                                       path = "/";
        replication = in.readShort();
        modificationTime = in.readLong();
        if (imgVersion <= -17) {
          atime = in.readLong();
        }
        if (imgVersion <= -8) {
          blockSize = in.readLong();
        }
        int numBlocks = in.readInt();        
        boolean flag = false;
        long size = 0;
        if ((-9 <= imgVersion && numBlocks > 0) ||
            (imgVersion < -9 && numBlocks >= 0)) {
          flag = true;
          for (int j = 0; j < numBlocks; j++) {
            if (-14 < imgVersion) {
               in.readLong();
               size += in.readLong();
            } else {
              block.readFields(in);
              size += block.getNumBytes();
            }
          }
        }
        
        // get quota only when the node is a directory
        if (imgVersion <= -16 && !flag) {
          in.readLong();
        }
        if (imgVersion <= -18 && !flag) {
          in.readLong();
        }
        
        PermissionStatus ps = null;
        if (imgVersion <= -11) {
          ps = PermissionStatus.read(in);
        }
        
        printFileInfo(path, !flag, ps, replication, size, modificationTime);
      }
      
    } finally {
      in.close();
    }
  }

  //Download the fsimage to local via http
       private void getFSImage(String fsName, String dir) {
               File file = new File(dir);
               if(file !=null && file.exists())
                       file.delete();

               String fileid = "getimage=1";
               try {
           FSImageFetcher.getFileClient(fsName, fileid, dir);
    } catch (IOException e) {
           e.printStackTrace();
           System.out.println("Fail to getFSImge from " + fsName);
    }
       }

       //Skip all useless info in one line
  private boolean skipUseless(DataInputStream in, int imgVersion) throws IOException {
               Block block = new Block();
       in.readShort();
               in.readLong();
               if (imgVersion <= -17) {
                       in.readLong();
               }
               if (imgVersion <= -8) {
                       in.readLong();
               }
               int numBlocks = in.readInt();
               boolean flag = false;

               if ((-9 <= imgVersion && numBlocks > 0)
                   || (imgVersion < -9 && numBlocks >= 0)) {
                       flag = true;
                       for (int j = 0; j < numBlocks; j++) {
                               if (-14 < imgVersion) {
                                       in.readLong();
                                       in.readLong();
                               } else {
                                       block.readFields(in);
                               }
                       }
               }
               if (imgVersion <= -16 && !flag) {
                       in.readLong();
               }
               if (imgVersion <= -18 && !flag) {
                       in.readLong();
               }

               if (imgVersion <= -11) {
                       PermissionStatus.read(in);
               }

               return !flag;
  }

  //Read the fsimage, and print out the path to a local file
       private boolean loadFSImage(String src, String dst) throws IOException {
               File curFile = new File(src);
               assert curFile != null && curFile.exists() : "curFile is null";

               DataInputStream in = new DataInputStream(new BufferedInputStream(
                   new FileInputStream(curFile)));

               BufferedWriter out = new BufferedWriter(new FileWriter(new File(dst)));

               try {
                       int imgVersion = in.readInt();
                       in.readInt();

                       long numFiles;
                       if (imgVersion <= -16) {
                               numFiles = in.readLong();
                       } else {
                               numFiles = in.readInt();
                       }

                       if (imgVersion <= -12) {
                               in.readLong();
                       }

                       String path;
                       for (long i = 0; i < numFiles; i++) {
                               path = readString(in);
                               if (path.length() == 0)
                                       path = "/";
                               boolean flag = this.skipUseless(in, imgVersion);
                               path+=" "+flag+"\n";
                               out.write(path);
                               out.flush();
                       }
               } finally {
                       in.close();
                       out.close();
               }
               return true;
       }

       //Compare two sorted file, print out the file with corruption
       public void compareImageFile(String onlinePath, String archivePath, String dst) throws IOException {
         File onlineFile = new File(onlinePath);
         File archiveFile = new File(archivePath);
         assert onlineFile !=null && onlineFile.exists() : "Online FSImage does not exist!";
         assert archiveFile !=null && archiveFile.exists() : "Archive FSImage does not exist!";
           
         BufferedReader onlineReader = new BufferedReader(new FileReader(onlineFile));
         BufferedReader archiveReader = new BufferedReader(new FileReader(archiveFile));
          
         FileWriter resultWriter = new FileWriter(new File(dst));
         try {
           String lineOn = onlineReader.readLine();
           String lineOff = archiveReader.readLine();
           
           while(true) {
               if (lineOn == null || lineOff == null)
                       break;
           
               String[] onStrings = lineOn.split(" ");
               String[] offStrings = lineOff.split(" ");
           
               String onName = onStrings[0];
               boolean isOnDir = Boolean.parseBoolean(onStrings[1]); 
               String offName = offStrings[0];
               boolean isOffDir = Boolean.parseBoolean(offStrings[1]);
           
               int result = onName.compareTo(offName); 
               if (result > 0) {
                       lineOff = archiveReader.readLine();
                       continue;
               }       else if (result < 0) { 
                       lineOn = onlineReader.readLine();
                       continue;
               } else {
                       /*if (isOnDir && isOffDir) {
                               //It's legal, do nothing;
                       } else if (isOnDir && !isOffDir) {
                         //It's legal, do nothing;
                       } else*/
                       if (!isOnDir && isOffDir) {
                               resultWriter.write("File and Directory Corruption\t"+ onName + "\t" + offName + "\n");
                               resultWriter.flush();
                       } else if (!isOnDir && !isOffDir) {
                               resultWriter.write("Same File Error\t"+ onName + "\t" + offName + "\n");
                               resultWriter.flush();
                       }
                       lineOn = onlineReader.readLine();
                       lineOff = archiveReader.readLine();
                       continue;
               }
           }
    } finally {
       resultWriter.close();
       onlineReader.close();
       archiveReader.close();
    }
       }

       //Print the Usage of this class
       private void printUsage() {
               System.out.println("A Tool for Checking the Online Cluster and Archive Cluster.");
               System.out.println("Warning: If you want to use the check function, plz make sure you sort the two input path respectively.");
               System.out.println("Usage: hadoop ArchiveChecker");
               System.out.println("           [-download <hdfsPath> <dst>]");
               System.out.println("           [-loadtolocal <src> <dst>]");
               System.out.println("           [-check <onlinePath> <archivePath> <dst>]");
               System.out.println("           [-lsr <imageName>]");
               System.out.println("           [-help]");
       }

       //Run the checker with input parameters
       public void runChecker(String[] argv) throws IOException {
               if (argv.length < 1) {
                       printUsage();
                       return;
               }

               String cmd = argv[0];
               if (cmd.equalsIgnoreCase("-help")) {
                       printUsage();
               }
               else if (cmd.equalsIgnoreCase("-download")) {
                       if (argv.length == 3) {
                               String hdfs = argv[1];
                               String dir = argv[2];
                               getFSImage(hdfs, dir);
                       }       else
                               printUsage();
               } else if (cmd.equalsIgnoreCase("-loadtolocal")) {
                       if (argv.length == 3) {
                               String src = argv[1];
                               String dst = argv[2];
                               loadFSImage(src, dst);
                       } else
                               printUsage();
               }       else if (cmd.equalsIgnoreCase("-check")) {
                       if (argv.length == 4) {
                               String onlinePath = argv[1];
                               String archivePath = argv[2];
                               String dst = argv[3];
                               compareImageFile(onlinePath, archivePath, dst);
                       } else
                               printUsage();
               } else if (cmd.equalsIgnoreCase("-lsr")) {
                       if (argv.length == 2) {
                               listFile(new File(argv[1]));
                       } else
                               printUsage();
               }
               else
                       printUsage();
       }

       public static void main(String[] argv) throws Exception{
               /*
                * Example:
                * ArchiveChecker -download localhost:50070 /tmp/r1
                * ArchiveChecker -loadtolocal /tmp/r1 /tmp/r2
                * ArchiveChecker -check /tmp/r2 /tmp/r4 /tmp/rr
                * ArchiveChecker -lsr imagename
                */
    ArchiveChecker ac = new ArchiveChecker();
    ac.runChecker(argv);
       }
}
