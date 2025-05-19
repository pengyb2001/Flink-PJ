package com.example.util;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 共享状态管理器
 * 为处理函数提供统一的状态访问接口
 */
public class SharedStateManager {
    // 状态键前缀
    private static final String KEY_CUSTOMER = "c:";  // 客户相关数据
    private static final String KEY_ORDER = "o:";     // 订单相关数据
    private static final String KEY_LINEITEM = "li:"; // 行项目相关数据
    
    // 可用的业务状态类型
    public static final String STATE_CUSTOMER_FILTERED = "filtered";  // 已筛选客户
    public static final String STATE_ORDER_VALID = "valid";           // 有效订单
    public static final String STATE_ORDER_LINEITEMS = "items";       // 订单行项目
    public static final String STATE_ORDER_CUSTOMER = "customer";     // 订单对应客户
    
    // 统一状态存储
    private final MapState<String, Object> stateStore;
    
    /**
     * 构造函数
     */
    public SharedStateManager(RuntimeContext context) throws Exception {
        MapStateDescriptor<String, Object> descriptor = 
            new MapStateDescriptor<>("unified-state-store", String.class, Object.class);
        stateStore = context.getMapState(descriptor);
    }
    
    /**
     * 添加客户到已筛选集合
     */
    @SuppressWarnings("unchecked")
    public void addFilteredCustomer(Long customerId) throws Exception {
        String key = KEY_CUSTOMER + STATE_CUSTOMER_FILTERED;
        Set<Long> filteredCustomers = (Set<Long>) stateStore.get(key);
        if (filteredCustomers == null) {
            filteredCustomers = new HashSet<>();
        }
        filteredCustomers.add(customerId);
        stateStore.put(key, filteredCustomers);
    }
    
    /**
     * 移除已筛选客户
     */
    @SuppressWarnings("unchecked")
    public void removeFilteredCustomer(Long customerId) throws Exception {
        String key = KEY_CUSTOMER + STATE_CUSTOMER_FILTERED;
        Set<Long> filteredCustomers = (Set<Long>) stateStore.get(key);
        if (filteredCustomers != null) {
            filteredCustomers.remove(customerId);
            if (filteredCustomers.isEmpty()) {
                stateStore.remove(key);
            } else {
                stateStore.put(key, filteredCustomers);
            }
        }
    }
    
    /**
     * 检查客户是否已筛选
     */
    @SuppressWarnings("unchecked")
    public boolean isCustomerFiltered(Long customerId) throws Exception {
        String key = KEY_CUSTOMER + STATE_CUSTOMER_FILTERED;
        Set<Long> filteredCustomers = (Set<Long>) stateStore.get(key);
        return filteredCustomers != null && filteredCustomers.contains(customerId);
    }
    
    /**
     * 标记订单为有效
     */
    public void markOrderValid(Long orderId, boolean valid) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = getOrCreateMap(key);
        orderData.put(STATE_ORDER_VALID, valid);
        stateStore.put(key, orderData);
    }
    
    /**
     * 关联订单与客户
     */
    public void linkOrderToCustomer(Long orderId, Long customerId) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = getOrCreateMap(key);
        orderData.put(STATE_ORDER_CUSTOMER, customerId);
        stateStore.put(key, orderData);
        
        // 在客户记录中添加订单引用
        addOrderToCustomer(customerId, orderId);
    }
    
    /**
     * 为客户添加订单引用
     */
    @SuppressWarnings("unchecked")
    private void addOrderToCustomer(Long customerId, Long orderId) throws Exception {
        String key = KEY_CUSTOMER + customerId;
        Map<String, Object> customerData = getOrCreateMap(key);
        Set<Long> orders = (Set<Long>) customerData.getOrDefault("orders", new HashSet<>());
        orders.add(orderId);
        customerData.put("orders", orders);
        stateStore.put(key, customerData);
    }
    
    /**
     * 获取客户的所有订单
     */
    @SuppressWarnings("unchecked")
    public Set<Long> getCustomerOrders(Long customerId) throws Exception {
        String key = KEY_CUSTOMER + customerId;
        Map<String, Object> customerData = getOrCreateMap(key);
        return (Set<Long>) customerData.getOrDefault("orders", new HashSet<>());
    }
    
    /**
     * 检查订单是否有效
     */
    @SuppressWarnings("unchecked")
    public boolean isOrderValid(Long orderId) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = (Map<String, Object>) stateStore.get(key);
        return orderData != null && Boolean.TRUE.equals(orderData.get(STATE_ORDER_VALID));
    }
    
    /**
     * 添加行项目到订单
     */
    @SuppressWarnings("unchecked")
    public void addLineItemToOrder(Long orderId, String itemKey, LineItemData item) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = getOrCreateMap(key);
        Map<String, LineItemData> items = (Map<String, LineItemData>) 
            orderData.getOrDefault(STATE_ORDER_LINEITEMS, new HashMap<>());
        items.put(itemKey, item);
        orderData.put(STATE_ORDER_LINEITEMS, items);
        stateStore.put(key, orderData);
    }
    
    /**
     * 移除订单的行项目
     */
    @SuppressWarnings("unchecked")
    public void removeLineItemFromOrder(Long orderId, String itemKey) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = (Map<String, Object>) stateStore.get(key);
        if (orderData != null) {
            Map<String, LineItemData> items = (Map<String, LineItemData>) 
                orderData.getOrDefault(STATE_ORDER_LINEITEMS, new HashMap<>());
            items.remove(itemKey);
            orderData.put(STATE_ORDER_LINEITEMS, items);
            stateStore.put(key, orderData);
        }
    }
    
    /**
     * 获取订单的所有行项目
     */
    @SuppressWarnings("unchecked")
    public Map<String, LineItemData> getOrderLineItems(Long orderId) throws Exception {
        String key = KEY_ORDER + orderId;
        Map<String, Object> orderData = (Map<String, Object>) stateStore.get(key);
        if (orderData == null) {
            return new HashMap<>();
        }
        return (Map<String, LineItemData>) 
            orderData.getOrDefault(STATE_ORDER_LINEITEMS, new HashMap<>());
    }
    
    /**
     * 删除订单及其所有数据
     */
    @SuppressWarnings("unchecked")
    public void removeOrder(Long orderId) throws Exception {
        String orderKey = KEY_ORDER + orderId;
        Map<String, Object> orderData = (Map<String, Object>) stateStore.get(orderKey);
        
        if (orderData != null) {
            // 获取客户ID并从客户记录中移除订单引用
            Long customerId = (Long) orderData.get(STATE_ORDER_CUSTOMER);
            if (customerId != null) {
                removeOrderFromCustomer(customerId, orderId);
            }
            
            // 删除订单记录
            stateStore.remove(orderKey);
        }
    }
    
    /**
     * 从客户记录中移除订单引用
     */
    @SuppressWarnings("unchecked")
    private void removeOrderFromCustomer(Long customerId, Long orderId) throws Exception {
        String key = KEY_CUSTOMER + customerId;
        Map<String, Object> customerData = (Map<String, Object>) stateStore.get(key);
        if (customerData != null) {
            Set<Long> orders = (Set<Long>) customerData.getOrDefault("orders", new HashSet<>());
            orders.remove(orderId);
            if (orders.isEmpty()) {
                customerData.remove("orders");
            } else {
                customerData.put("orders", orders);
            }
            
            if (customerData.isEmpty()) {
                stateStore.remove(key);
            } else {
                stateStore.put(key, customerData);
            }
        }
    }
    
    /**
     * 获取或创建状态Map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getOrCreateMap(String key) throws Exception {
        Map<String, Object> map = (Map<String, Object>) stateStore.get(key);
        return map != null ? map : new HashMap<>();
    }
    
    /**
     * 行项目数据类
     */
    public static class LineItemData {
        private final LocalDate shipDate;
        private final double extPrice;
        private final double discountRate;
        
        public LineItemData(LocalDate shipDate, double extPrice, double discountRate) {
            this.shipDate = shipDate;
            this.extPrice = extPrice;
            this.discountRate = discountRate;
        }
        
        public LocalDate getShipDate() {
            return shipDate;
        }
        
        public double getExtPrice() {
            return extPrice;
        }
        
        public double getDiscountRate() {
            return discountRate;
        }
        
        @Override
        public int hashCode() {
            return 31 * (31 * shipDate.hashCode() + Double.hashCode(extPrice)) + Double.hashCode(discountRate);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LineItemData)) return false;
            LineItemData other = (LineItemData) obj;
            return shipDate.equals(other.shipDate) 
                && Double.compare(extPrice, other.extPrice) == 0
                && Double.compare(discountRate, other.discountRate) == 0;
        }
    }
} 