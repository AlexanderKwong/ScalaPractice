package com.qtone.config;

import com.qtone.common.spring.ISpringContext;
import com.qtone.common.spring.SpringUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;


/**
 * @author 邝晓林
 * @version V1.0
 * @Description
 * @Company 广东全通教育股份公司
 * @date 2016/10/25
 */
@Component
public class CustomizeAnnotationProxy implements BeanPostProcessor, ApplicationContextAware, ISpringContext {

    private ApplicationContext applicationContext;

    public CustomizeAnnotationProxy(){
        try {
            SpringUtil.initSprintUtil(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String s) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String s) throws BeansException {
        return bean;
    }

    @Override
    public <T> T getSpringBean(Class<T> clazz, String name) {
        return (T)applicationContext.getBean(name);
    }
}
