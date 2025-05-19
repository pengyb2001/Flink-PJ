package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.LineItem;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Objects;

/**
 * 行项目分析处理器
 * 连接订单与行项目数据流，筛选满足时间条件的行项目并计算相关指标
 */
public class LineitemProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<LineItem, String>, Tuple4<LocalDate, Double, Double, String>> {
    
    // 事件类型标记
    private static final String EVENT_ADD = "+";
    private static final String EVENT_REMOVE = "-";
    
    // 时间阈值常量
    private static final LocalDate SHIPMENT_DATE_BOUNDARY = LocalDate.parse("1995-03-13");
    
    // 订单及行项目映射状态
    private MapState<Long, List<ItemDetails>> orderLineitemsRegistry;
    
    // 有效订单跟踪状态
    private ValueState<Set<Long>> validOrdersRegistry;
    
    /**
     * 行项目详情内部类
     */
    private static class ItemDetails {
        final LocalDate shipDate;
        final double extPrice;
        final double discountRate;
        
        ItemDetails(LocalDate shipDate, double extPrice, double discountRate) {
            this.shipDate = shipDate;
            this.extPrice = extPrice;
            this.discountRate = discountRate;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ItemDetails)) return false;
            ItemDetails other = (ItemDetails) obj;
            return Objects.equals(shipDate, other.shipDate) && 
                   Double.compare(extPrice, other.extPrice) == 0 && 
                   Double.compare(discountRate, other.discountRate) == 0;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(shipDate, extPrice, discountRate);
        }
        
        Tuple3<LocalDate, Double, Double> toTuple() {
            return new Tuple3<>(shipDate, extPrice, discountRate);
        }
    }

    /**
     * 初始化状态组件
     */
    @Override
    public void open(Configuration params) throws Exception {
        // 订单到行项目映射的状态描述符
        MapStateDescriptor<Long, List<ItemDetails>> orderItemsDescriptor = 
            new MapStateDescriptor<>("order-lineitems-registry", Long.class, 
                                    (Class<List<ItemDetails>>) (Class<?>) List.class);
        orderLineitemsRegistry = getRuntimeContext().getMapState(orderItemsDescriptor);
        
        // 有效订单集合的状态描述符
        ValueStateDescriptor<Set<Long>> validOrdersDescriptor = 
            new ValueStateDescriptor<>("valid-orders-registry", 
                                       (Class<Set<Long>>) (Class<?>) Set.class);
        validOrdersRegistry = getRuntimeContext().getState(validOrdersDescriptor);
    }

    /**
     * 处理订单流元素
     */
    @Override
    public void processElement1(Tuple2<Long, String> orderEvent, Context ctx, Collector<Tuple4<LocalDate, Double, Double, String>> results) throws Exception {
        Long orderId = orderEvent.f0;
        String eventType = orderEvent.f1;
        
        // 获取或初始化有效订单集合
        Set<Long> validOrders = getOrCreateValidOrders();
        
        // 根据事件类型更新有效订单集合
        if (EVENT_ADD.equals(eventType)) {
            validOrders.add(orderId);
        } else if (EVENT_REMOVE.equals(eventType)) {
            validOrders.remove(orderId);
        }
        
        // 保存更新后的有效订单集合
        validOrdersRegistry.update(validOrders);
        
        // 如果该订单有行项目，则发送所有相关行项目
        emitLineItemsForOrder(orderId, eventType, results);
    }

    /**
     * 处理行项目流元素
     */
    @Override
    public void processElement2(Tuple2<LineItem, String> lineItemEvent, Context ctx, Collector<Tuple4<LocalDate, Double, Double, String>> results) throws Exception {
        LineItem lineItem = lineItemEvent.f0;
        String eventType = lineItemEvent.f1;
        
        // 提取行项目信息
        Long orderId = lineItem.getLOrderkey();
        LocalDate shipDate = lineItem.getLShipDate();
        double extPrice = lineItem.getLExtendedprice();
        double discount = lineItem.getLDiscount();
        
        // 检查发货日期是否满足条件（需要晚于边界日期）
        if (!shipDate.isAfter(SHIPMENT_DATE_BOUNDARY)) {
            return; // 不满足条件，直接返回
        }
        
        // 创建行项目详情对象
        ItemDetails itemDetails = new ItemDetails(shipDate, extPrice, discount);
        
        // 更新订单的行项目集合
        updateOrderLineItems(orderId, itemDetails, eventType);
        
        // 检查订单是否有效，如果有效则发送该行项目
        emitItemIfOrderValid(orderId, itemDetails, eventType, results);
    }
    
    /**
     * 获取或创建有效订单集合
     */
    private Set<Long> getOrCreateValidOrders() throws Exception {
        Set<Long> validOrders = validOrdersRegistry.value();
        if (validOrders == null) {
            validOrders = new HashSet<>();
        }
        return validOrders;
    }
    
    /**
     * 发送订单相关的所有行项目
     */
    private void emitLineItemsForOrder(Long orderId, String eventType, 
                                     Collector<Tuple4<LocalDate, Double, Double, String>> results) throws Exception {
        List<ItemDetails> lineItems = orderLineitemsRegistry.get(orderId);
        if (lineItems != null && !lineItems.isEmpty()) {
            for (ItemDetails item : lineItems) {
                results.collect(new Tuple4<>(
                    item.shipDate, item.extPrice, item.discountRate, eventType
                ));
            }
        }
    }
    
    /**
     * 更新订单的行项目集合
     */
    private void updateOrderLineItems(Long orderId, ItemDetails itemDetails, String eventType) throws Exception {
        List<ItemDetails> lineItems = orderLineitemsRegistry.get(orderId);
        
        if (EVENT_ADD.equals(eventType)) {
            // 添加行项目
            if (lineItems == null) {
                lineItems = new ArrayList<>();
            }
            lineItems.add(itemDetails);
            orderLineitemsRegistry.put(orderId, lineItems);
        } else if (EVENT_REMOVE.equals(eventType)) {
            // 移除行项目
            if (lineItems != null) {
                lineItems.remove(itemDetails);
                if (lineItems.isEmpty()) {
                    orderLineitemsRegistry.remove(orderId);
                } else {
                    orderLineitemsRegistry.put(orderId, lineItems);
                }
            }
        }
    }
    
    /**
     * 如果订单有效则发送行项目
     */
    private void emitItemIfOrderValid(Long orderId, ItemDetails itemDetails, String eventType, 
                                    Collector<Tuple4<LocalDate, Double, Double, String>> results) throws Exception {
        Set<Long> validOrders = validOrdersRegistry.value();
        if (validOrders != null && validOrders.contains(orderId)) {
            results.collect(new Tuple4<>(
                itemDetails.shipDate, itemDetails.extPrice, itemDetails.discountRate, eventType
            ));
        }
    }
} 