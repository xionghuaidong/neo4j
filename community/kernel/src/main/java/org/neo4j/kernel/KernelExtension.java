/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.neo4j.helpers.Service;

/**
 * Hook for providing extended functionality to the Neo4j Graph Database kernel.
 *
 * Implementations of {@link KernelExtension} must fulfill the following
 * contract:
 * <ul>
 * <li>Must have a public no-arg constructor.</li>
 * <li>The same instance must be able to load with multiple GraphDatabase
 * kernels.</li>
 * <li>Different instances should be able to access the same state from the same
 * kernel. <br/>
 * To achieve this, different instances of the same class should have the same
 * {@link #hashCode()} and be {@link #equals(Object)}. This is enforced by this
 * base class by delegating {@link #hashCode()} and {@link #equals(Object)} to
 * the same methods on the {@link #getClass() class object}.</li>
 * </ul>
 * The simplest way to implement an {@link KernelExtension extension} that
 * fulfills this contract is if the {@link KernelExtension extension}
 * implementation is stateless, and all state is kept in the state object
 * returned by {@link #load(KernelData) the load method}.
 *
 * Note that for an {@link KernelExtension extension} to be considered loaded by
 * the kernel, {@link #load(KernelData) the load method} may not return
 * <code>null</code>. {@link #unload(Object) The unload method} will only be
 * invoked if the kernel considers the {@link KernelExtension extension} loaded.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 * @param <S> the Extension state type
 */
public abstract class KernelExtension<S> extends Service
{
    static final String INSTANCE_ID = "instanceId";

    public KernelExtension( String key )
    {
        super( key );
    }

    @Override
    public final int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public final boolean equals( Object obj )
    {
        return this.getClass().equals( obj.getClass() );
    }

    public final void loadAgent( String agentArgs )
    {
        KernelData.visitAll( this, agentArgument( agentArgs ) );
    }

    protected final void loadAgent( KernelData kernel, Object param )
    {
        kernel.accept( this, param );
    }

    protected Object agentArgument( String agentArg )
    {
        return agentArg;
    }

    /**
     * Load this extension for a particular Neo4j Kernel.
     */
    protected abstract S load( KernelData kernel );

    protected S agentLoad( KernelData kernel, Object param )
    {
        S state = load( kernel );
        agentVisit( kernel, state, param );
        return state;
    }

    protected void agentVisit( KernelData kernel, S state, Object param )
    {
        // override to to define behavior
    }

    /**
     * Takes place before any data sources has been registered and is there to
     * let extensions affect the configuration of other things starting up.
     */
    protected void loadConfiguration( KernelData kernel )
    {
        // Default: do nothing
    }

    protected void unload( S state )
    {
        // Default: do nothing
    }

    protected final S getState( KernelData kernel )
    {
        return kernel.getState( this );
    }

    protected boolean isLoaded( KernelData kernel )
    {
        return getState( kernel ) != null;
    }

    public class Function<T>
    {
        private final Class<T> type;
        private final KernelData kernel;
        private final Method method;

        private Function( Class<T> type, KernelData kernel, Method method )
        {
            this.type = type;
            this.kernel = kernel;
            this.method = method;
        }

        public T call( Object... args )
        {
            Object[] arguments = new Object[args == null ? 1 : ( args.length + 1 )];
            arguments[0] = kernel;
            if ( args != null && args.length > 0 )
            {
                System.arraycopy( args, 0, arguments, 1, args.length );
            }
            try
            {
                return type.cast( method.invoke( KernelExtension.this, arguments ) );
            }
            catch ( IllegalAccessException e )
            {
                throw new IllegalStateException( "Access denied", e );
            }
            catch ( InvocationTargetException e )
            {
                Throwable exception = e.getTargetException();
                if ( exception instanceof RuntimeException )
                {
                    throw (RuntimeException) exception;
                }
                else if ( exception instanceof Error )
                {
                    throw (Error) exception;
                }
                else
                {
                    throw new RuntimeException( "Unexpected exception: " + exception.getClass(), exception );
                }
            }
        }
    }

    protected <T> Function<T> function( KernelData kernel, String name, Class<T> result, Class<?>... params )
    {
        Class<?>[] parameters = new Class[params == null ? 1 : ( params.length + 1 )];
        parameters[0] = KernelData.class;
        if ( params != null && params.length != 0 )
        {
            System.arraycopy( params, 0, parameters, 1, params.length );
        }
        final Method method;
        try
        {
            method = getClass().getMethod( name, parameters );
            /* if ( !result.isAssignableFrom( method.getReturnType() ) ) return null; */
            if ( !Modifier.isPublic( method.getModifiers() ) ) return null;
        }
        catch ( Exception e )
        {
            return null;
        }
        return new Function<T>( result, kernel, method );
    }
}
