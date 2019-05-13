package me.inojgn.demobatchpartitioning.domain.sales;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@NoArgsConstructor
@Table
public class SalesRecord {

    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column
    private String region;

    @Column
    private String country;

    @Column
    private String itemType;

    @Column
    private String salesChannel;

    @Column
    private String orderPriority;

    @Column
    private String orderDate;

    @Column(name = "orderId", length = 20, unique = true)
    private String orderId;

    @Column
    private String shipDate;

    @Column
    private int unitsSold;

    @Column
    private double unitPrice;

    @Column
    private double unitCost;

    @Column
    private double totalRevenue;

    @Column
    private double totalCost;

    @Column
    private double totalProfit;

    @Builder
    public SalesRecord(String region, String country, String itemType, String salesChannel, String orderPriority, String orderDate, String orderId, String shipDate, int unitsSold, double unitPrice, double unitCost, double totalRevenue, double totalCost, double totalProfit) {
        this.region = region;
        this.country = country;
        this.itemType = itemType;
        this.salesChannel = salesChannel;
        this.orderPriority = orderPriority;
        this.orderDate = orderDate;
        this.orderId = orderId;
        this.shipDate = shipDate;
        this.unitsSold = unitsSold;
        this.unitPrice = unitPrice;
        this.unitCost = unitCost;
        this.totalRevenue = totalRevenue;
        this.totalCost = totalCost;
        this.totalProfit = totalProfit;
    }
}
