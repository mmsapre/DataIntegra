package com.integration.em.utils;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
public class P {

    public static <T> boolean CollectionContainsValue(Collection<T> collection, T value) {
        return new Contains<T>(value).invoke(collection);
    }

    public static class Contains<T> implements Func<Boolean, Collection<T>> {

        private T value;

        public Contains(T value) {
            this.value = value;
        }


        @Override
        public Boolean invoke(Collection<T> in) {
            return in.contains(value);
        }

    }

    public static <T> boolean CollectionContainsAllValues(Collection<T> collection, Collection<T> values) {
        return new ContainsAll<>(values).invoke(collection);
    }

    public static class ContainsAll<T> implements Func<Boolean, Collection<T>> {

        private Collection<T> value;

        public ContainsAll(Collection<T> value) {
            this.value = value;
        }


        @Override
        public Boolean invoke(Collection<T> in) {
            return in.containsAll(value);
        }

    }

    public static <T> boolean SetEqualsValues(Collection<T> collection, Set<T> values) {
        return new SetEquals<>(values).invoke(collection);
    }

    public static class SetEquals<T> implements Func<Boolean, Collection<T>> {

        private Set<T> value;


        public SetEquals(Set<T> value) {
            this.value = value;
        }


        @Override
        public Boolean invoke(Collection<T> in) {
            return new HashSet<T>(in).equals(value);
        }
    }

    public static <T> boolean ValueIsContainedInCollection(Collection<T> collection, T value) {
        return new IsContainedIn<>(collection).invoke(value);
    }

    public static class IsContainedIn<T> implements Func<Boolean, T> {

        private Collection<T> value;

        public IsContainedIn(Collection<T> value) {
            this.value = value;
        }


        @Override
        public Boolean invoke(T in) {
            return value.contains(in);
        }

    }

    public static <T> boolean CollectionIsContainedInValues(Collection<T> collection, Collection<T> values) {
        return new AreAllContainedIn<>(values).invoke(collection);
    }

    public static class AreAllContainedIn<T> implements Func<Boolean, Collection<T>> {

        private Collection<T> value;

        public AreAllContainedIn(Collection<T> value) {
            this.value = value;
        }


        @Override
        public Boolean invoke(Collection<T> in) {
            return value.containsAll(in);
        }

    }
}
