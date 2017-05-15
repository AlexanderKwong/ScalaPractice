package com.qtone.common.spring;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spring 容器
 * @author Ethan.Lam  2011-7-12
 *
 */
public class SpringUtil {
	
	private static ISpringContext springUtil;

	private static final Logger LOG = LoggerFactory.getLogger(SpringUtil.class);
	
	public  static void initSprintUtil(ISpringContext pUtl) throws Exception
	{
		if(pUtl==null){
			throw new Exception();
	    }
//		System.out.println("成功初始化SpringUtil");
		LOG.info("成功初始化SpringUtil");
		springUtil=pUtl;
	}

	/**
	 * 获取spring 管理的bean
	 * 
	 * @return
	 */
	public static <T> T getSpringBean(Class<T> clazz,String name) {
		return springUtil.getSpringBean(clazz, name);
	}



	
	
	
}
