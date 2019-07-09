package org.fog.entities;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

  private static final int REPLICATION_FACTOR = 200;

  public static class Point2D {
      
      private double x;
      private double y;
      

      public Point2D(double x, double y) {
          this.x = x;
          this.y = y;
      }
      
      public Point2D() {
		// TODO Auto-generated constructor stub
	}

	private double getDistance(Point2D other) {
          return Math.sqrt(Math.pow(this.x - other.x, 2)
                  + Math.pow(this.y - other.y, 2));
      }
      
      public int getNearestPointIndex(List<Point2D> points) {
          int index = -1;
          double minDist = Double.MAX_VALUE;
          for (int i = 0; i < points.size(); i++) {
              double dist = this.getDistance(points.get(i));
              if (dist < minDist) {
                  minDist = dist;
                  index = i;
              }
          }
          return index;
      }
      
      public static Point2D getMean(List<Point2D> points) {
          double accumX = 0;
          double accumY = 0;
          if (points.size() == 0) return new Point2D(accumX, accumY);
          for (Point2D point : points) {
              accumX += point.x;
              accumY += point.y;
          }
          return new Point2D(accumX / points.size(), accumY / points.size());
      }
      
      @Override
      public String toString() {
          return "[" + this.x + "," + this.y + "]";
      }
      
      @Override
      public boolean equals(Object obj) {
          if (obj == null || !(obj.getClass() != Point2D.class)) {
              return false;
          }
          Point2D other = (Point2D) obj;
          return this.x == other.x && this.y == other.y;
      }
      
  }

  public static List<Point2D> getDataset(double x , double y) throws Exception {
      List<Point2D> dataset = new ArrayList<>();
     
      for (int i = 0; i < REPLICATION_FACTOR; i++) {
          Point2D point = new Point2D(x, y);
          dataset.add(point);
      }
      return dataset;
  }
  
  public static List<Point2D> initializeRandomCenters(int n, int lowerBound, int upperBound) {
      List<Point2D> centers = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
          double x = (double)(Math.random() * (upperBound - lowerBound) + lowerBound);
          double y = (double)(Math.random() * (upperBound - lowerBound) + lowerBound);
          Point2D point = new Point2D(x, y);
          centers.add(point);
      }
      return centers;
  }

  public static List<Point2D> getNewCenters(List<Point2D> dataset, List<Point2D> centers) {
      List<List<Point2D>> clusters = new ArrayList<>(centers.size());
      for (int i = 0; i < centers.size(); i++) {
          clusters.add(new ArrayList<Point2D>());
      }
      for (Point2D data : dataset) {
          int index = data.getNearestPointIndex(centers);
          clusters.get(index).add(data);
      }
      List<Point2D> newCenters = new ArrayList<>(centers.size());
      for (List<Point2D> cluster : clusters) {
          newCenters.add(Point2D.getMean(cluster));
      }
      return newCenters;
  }
  
  public static double getDistance(List<Point2D> oldCenters, List<Point2D> newCenters) {
      double accumDist = 0;
      for (int i = 0; i < oldCenters.size(); i++) {
          double dist = oldCenters.get(i).getDistance(newCenters.get(i));
          accumDist += dist;
      }
      return accumDist;
  }
  
  public static List<Point2D> kmeans(List<Point2D> centers, List<Point2D> dataset, int k) {
      boolean converged;
      do {
          List<Point2D> newCenters = getNewCenters(dataset, centers);
          double dist = getDistance(centers, newCenters);
          centers = newCenters;
          converged = dist == 0;
      } while (!converged);
      return centers;
  }

  /*public static void main(String[] args) {
      
      
      String inputFile="C:\Users\MMTSHALI1\Desktop\Masters_Code\Kmeans\dataset";
      int k = Integer.valueOf(15);
      List<Point2D> dataset = null;
      try {
          dataset = getDataset(inputFile);
      } catch (Exception e) {
          System.err.println("ERROR: Could not read file " + inputFile);
          System.exit(-1);
      }
      List<Point2D> centers = initializeRandomCenters(k, 0, 1000000);
      long start = System.currentTimeMillis();
      kmeans(centers, dataset, k);
      System.out.println("Time elapsed: " + (System.currentTimeMillis() - start) + "ms");
      System.exit(0);
  }*/

}