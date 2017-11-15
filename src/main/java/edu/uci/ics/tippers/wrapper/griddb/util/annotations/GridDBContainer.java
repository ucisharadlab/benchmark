package edu.uci.ics.tippers.wrapper.griddb.util.annotations;

/**
 * Created by peeyush on 17/10/16.
 */
import com.toshiba.mwcloud.gs.ContainerType;
import edu.uci.ics.tippers.wrapper.griddb.util.KeyStoreType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface GridDBContainer
{
    ContainerType type() default ContainerType.COLLECTION;
    KeyStoreType keyStore() default KeyStoreType.STANDALONE;
    String storeName() default "";
}

