package qeorm;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.beans.BeanUtils;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import qeorm.annotation.Transient;
import qeorm.intercept.IFunIntercept;
import qeorm.utils.ExtendUtils;
import qeorm.utils.JsonUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by asheng on 2015/7/20 0020.
 */
public class MongodbModelBase extends ModelBase {
    protected boolean primaryKeyIntoDb() {
        return false;
    }
}
