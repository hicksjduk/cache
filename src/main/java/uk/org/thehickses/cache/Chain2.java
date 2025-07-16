package uk.org.thehickses.cache;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

public class Chain2
{
    public static <T> SupplierChain<T> of(Supplier<T> func)
    {
        return func::get;
    }

    public static IntSupplierChain of(IntSupplier func)
    {
        return func::getAsInt;
    }

    public static <T, R> FunctionChain<T, R> of(Function<T, R> func)
    {
        return func::apply;
    }

    public static <T> IntFunctionChain<T> of(IntFunction<T> func)
    {
        return func::apply;
    }

    public static <T> ToIntFunctionChain<T> of(ToIntFunction<T> func)
    {
        return func::applyAsInt;
    }

    public static <T> UnaryOperatorChain<T> of(UnaryOperator<T> func)
    {
        return func::apply;
    }

    public static IntUnaryOperatorChain of(IntUnaryOperator func)
    {
        return func::applyAsInt;
    }

    public static interface SupplierChain<T> extends Supplier<T>
    {
        default Runnable and(Consumer<? super T> func)
        {
            return () -> func.accept(get());
        }

        default <R> SupplierChain<R> and(Function<? super T, R> func)
        {
            return () -> func.apply(get());
        }

        default SupplierChain<T> and(UnaryOperator<T> func)
        {
            return () -> func.apply(get());
        }

        default IntSupplierChain and(ToIntFunction<? super T> func)
        {
            return () -> func.applyAsInt(get());
        }
    }

    public static interface IntSupplierChain extends IntSupplier
    {
        public default Runnable and(IntConsumer func)
        {
            return () -> func.accept(getAsInt());
        }
        
        public default <T> SupplierChain<T> and(IntFunction<T> func)
        {
            return () -> func.apply(getAsInt());
        }
        
        public default IntSupplierChain and(IntUnaryOperator func)
        {
            return () -> func.applyAsInt(getAsInt());
        }
    }

    public static interface FunctionChain<T, R> extends Function<T, R>
    {
        public default Consumer<T> and(Consumer<? super R> func)
        {
            return arg -> func.accept(apply(arg));
        }

        public default <S> FunctionChain<T, S> and(Function<? super R, S> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default FunctionChain<T, R> and(UnaryOperator<R> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default ToIntFunctionChain<T> and(ToIntFunction<? super R> func)
        {
            return arg -> func.applyAsInt(apply(arg));
        }
    }

    public static interface IntFunctionChain<T> extends IntFunction<T>
    {
        public default IntConsumer and(Consumer<T> func)
        {
            return arg -> func.accept(apply(arg));
        }
        
        public default <R> IntFunctionChain<R> and(Function<? super T, R> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default IntFunctionChain<T> and(UnaryOperator<T> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default IntUnaryOperatorChain and(ToIntFunction<? super T> func)
        {
            return arg -> func.applyAsInt(apply(arg));
        }
    }

    public static interface UnaryOperatorChain<T> extends UnaryOperator<T>
    {
        public default Consumer<T> and(Consumer<? super T> func)
        {
            return arg -> func.accept(apply(arg));
        }
        
        public default <R> FunctionChain<T, R> and(Function<? super T, R> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default UnaryOperatorChain<T> and(UnaryOperator<T> func)
        {
            return arg -> func.apply(apply(arg));
        }
        
        public default ToIntFunctionChain<T> and(ToIntFunction<T> func)
        {
            return arg -> func.applyAsInt(apply(arg));
        }
    }

    public static interface IntUnaryOperatorChain extends IntUnaryOperator
    {
        public default IntConsumer and(IntConsumer func)
        {
            return arg -> func.accept(applyAsInt(arg));
        }
        
        public default <T> IntFunctionChain<T> and(IntFunction<T> func)
        {
            return arg -> func.apply(applyAsInt(arg));
        }
        
        public default IntUnaryOperatorChain and(IntUnaryOperator func)
        {
            return arg -> func.applyAsInt(applyAsInt(arg));
        }
    }

    public static interface ToIntFunctionChain<T> extends ToIntFunction<T>
    {
        public default Consumer<T> and(IntConsumer func)
        {
            return arg -> func.accept(applyAsInt(arg));
        }
        
        public default <R> FunctionChain<T, R> and(IntFunction<R> func)
        {
            return arg -> func.apply(applyAsInt(arg));
        }
        
        public default ToIntFunctionChain<T> and(IntUnaryOperator func)
        {
            return arg -> func.applyAsInt(applyAsInt(arg));
        }
    }
}
