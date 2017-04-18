package cn.jw.rms.data.framework.core;

import cn.jw.rms.data.framework.common.rules.CleanerAssembly;
import cn.jw.rms.data.framework.common.rules.PredictModelAssembly;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by deanzhang on 15/11/28.
 */
public class AssemblyLoader {
    public static CleanerAssembly loadCleaner(String jarFilePath, String classPath) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {
            File myJar = new File(jarFilePath);
            URL url = myJar.toURI().toURL();
            Class[] parameters = new Class[]{URL.class};
            URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class<URLClassLoader> sysClass = URLClassLoader.class;
            Method method = sysClass.getDeclaredMethod("addURL", parameters);
            method.setAccessible(true);
            method.invoke(sysLoader, url);
            Constructor<CleanerAssembly> cs = (Constructor<CleanerAssembly>) ClassLoader.getSystemClassLoader().loadClass(classPath).getConstructor();
        return cs.newInstance();

    }

    public static PredictModelAssembly loadModel(String jarFilePath, String classPath) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, MalformedURLException, ClassNotFoundException {
        File myJar = new File(jarFilePath);
        URL url = myJar.toURI().toURL();
        Class[] parameters = new Class[]{URL.class};
        URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class<URLClassLoader> sysClass = URLClassLoader.class;
        Method method = sysClass.getDeclaredMethod("addURL", parameters);
        method.setAccessible(true);
        method.invoke(sysLoader, url);
        Constructor<PredictModelAssembly> cs = (Constructor<PredictModelAssembly>) ClassLoader.getSystemClassLoader().loadClass(classPath).getConstructor();
        return cs.newInstance();

    }
}
