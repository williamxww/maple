package com.bow.maple.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通过ExtensionLoader来管理各个组件实例<br/>
 * 可通过注解@SPI为接口指定默认的实现类 如@SPI("类名")<br/>
 * 每个ExtensionLoader对应接口type 和所有该接口的实现的实例instances
 *
 * @author vv
 * @since 2016/10/2.
 */
public class ExtensionLoader<T> {

    private static Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);

    /**
     * 一个class对应一个ExtensionLoader
     */
    private static ConcurrentMap<Class<?>, ExtensionLoader<?>> extensionLoaders = new ConcurrentHashMap();

    /**
     * 接口名
     */
    private Class<T> type;

    /**
     * {@link #type}对应的ExtensionLoader拥有各种实例
     */
    private Map<String, T> instances = new HashMap<String, T>();

    private ExtensionLoader(Class<T> type) {
        this.type = type;
        ServiceLoader serviceLoader = ServiceLoader.load(type);
        Iterator<T> it = serviceLoader.iterator();
        while (it.hasNext()) {
            T service = it.next();
            putInstance(service);
        }
    }

    private void putInstance(T service){
        // service 有名字，就用其名字存储，否则用其类名存储
        if (service.getClass().isAnnotationPresent(Name.class)) {
            Name nameAnnotation = service.getClass().getAnnotation(Name.class);
            String name = nameAnnotation.value();
            instances.put(name, service);
        } else {
            // 小写类名的第一个字母
            String name = this.getClass().getSimpleName();
            String first = name.substring(0, 1);
            String remain = name.substring(1);
            instances.put(first.toLowerCase() + remain, service);
        }
    }

    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        ExtensionLoader<T> loader = (ExtensionLoader<T>) extensionLoaders.get(type);
        if (loader == null) {
            loader = createExtensionLoader(type);
        }
        return loader;
    }

    private static synchronized <T> ExtensionLoader<T> createExtensionLoader(Class<T> type) {
        ExtensionLoader<T> loader = (ExtensionLoader<T>) extensionLoaders.get(type);
        if (loader == null) {
            loader = new ExtensionLoader<>(type);
            extensionLoaders.putIfAbsent(type, loader);
            loader = (ExtensionLoader<T>) extensionLoaders.get(type);
        }
        return loader;
    }

    /**
     * 通过接口上{@link SPI}注解获取默认实现
     * 
     * @return 某接口的默认实现
     */
    public T getExtension() {
        return getExtension(null);
    }

    /**
     * 根据名字获取接口的实现
     * 
     * @param name 实例的名字
     * @return 接口实例
     */
    public T getExtension(String name) {
        T result = null;
        // 如果name为空就使用 SPI 指定的默认实现
        if (StringUtils.isEmpty(name) && type.isAnnotationPresent(SPI.class)) {
            SPI spi = type.getAnnotation(SPI.class);
            name = spi.value();
        }
        //没有指定名字的时候，返回第一个
        if (StringUtils.isEmpty(name)) {
            Iterator<T> it = instances.values().iterator();
            if(it.hasNext()){
                result = it.next();
            }
        }else{
            // 忽略大小写
            for (String key : instances.keySet()) {
                if (name.equalsIgnoreCase(key)) {
                    name = key;
                }
            }
            result = instances.get(name);
        }

        if (result == null) {
            logger.warn("get nothing for type " + type.getName() + " name " + name);
        }
        return result;
    }

    /**
     * 通过编程动态的往容器中放入组件实例.如果名字相同会覆盖之前已经创建的实例。
     * 
     * @param name 组件名字
     * @param service 组件实例
     * @return 组件实例
     */
    public T putExtension(String name, T service) {
        instances.put(name, service);
        return service;
    }

    /**
     * 通过编程动态的往容器中放入组件实例.如果名字相同会覆盖之前已经创建的实例。
     * 
     * @param service 组件实例
     * @return 组件实例
     */
    public T putExtension(T service) {
        putInstance(service);
        return service;
    }
}
